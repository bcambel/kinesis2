(ns kinesis2.core
  (:require
    [clojure.string                         :as s]
    [clojure.java.io                        :as io]
    [clojure.tools.cli                      :refer [parse-opts]]
    [ring.adapter.jetty                     :refer [run-jetty]]
    [compojure.route                        :as route]
    [compojure.core                         :refer [defroutes GET POST DELETE ANY HEAD context]]
    [compojure.handler                      :refer [site]]
    [cheshire.core                          :refer :all]
    [amazonica.core                         :refer [with-credential defcredential]]
    [amazonica.aws.kinesis                  :as kinesis]
    [amazonica.aws.s3                       :as s3]
    [clojure.core.async                     :as async :refer [alts! go chan >!]]
    [com.stuartsierra.component             :as component]
    [metrics.core                           :refer [new-registry]]
    [metrics.meters                         :refer (meter mark! defmeter rates)]
    [metrics.histograms                     :refer [defhistogram update! percentiles mean std-dev number-recorded]]
    [metrics.timers                         :refer [deftimer time!] :as timers]
    [metrics.reporters.console              :as console]
    [metrics.reporters.jmx                  :as jmx]
    [kinesis2.log                           :as log-base]
    [byte-streams                           :refer [convert print-bytes]]
    [kinesis2.db                            :as db]
    [taoensso.timbre                        :as timbre
         :refer (log trace debug info warn error fatal report sometimes)])
  (:import java.util.zip.GZIPOutputStream)
  (:gen-class))

(def reg (new-registry))
(defmeter  message-ingested )
(defmeter  s3-uploads )
(defhistogram  queue-size)
(deftimer s3-upload-timing)

(def JR (jmx/reporter {}))
(def CR (console/reporter {}))

(defn new-q [] (java.util.concurrent.ConcurrentLinkedDeque.))

(defrecord HTTP [port listener conf server]
  component/Lifecycle

  (start [this]
    (info "Starting HTTP Component")
    (let []
      (try
        (defroutes app-routes
          (HEAD "/" [] "")
          (GET "/"  request {:status  200 :body "ok" })
          (GET "/ping" request {:status  200 :body "pong" })
          (GET "/stats" request {:status 200 :headers {"Content-Type" "application/json"}
                                             :body (generate-string {
                                                      :events (rates message-ingested)
                                                      :s3-uploads (rates s3-uploads)
                                                      :s3-upload-timing {:percentile (timers/percentiles s3-upload-timing)
                                                                          :calls (timers/number-recorded s3-upload-timing)
                                                                          :min (/ (timers/smallest s3-upload-timing) 100000) ;ms
                                                                          :std-dev (/ (timers/std-dev s3-upload-timing) 100000) ;ms
                                                                          :mean (/ (timers/mean s3-upload-timing) 100000) ;ms
                                                                          }
                                                      :buffer {:percentiles (percentiles queue-size)
                                                                :mean (mean queue-size)
                                                                :std-dev (std-dev queue-size)
                                                                :records (number-recorded queue-size)
                                                                }
                                                      })})
          (route/not-found "<p>Page not found.</p>"))

        (let [server (run-jetty (site #'app-routes) {:port port :join? false})]
          (info "Listening events now...")
          (assoc this :server server))
      (catch Throwable t
        (do
          (error t))))))

  (stop [this]
    (.stop server)))

(defn gen-temp-file []
  (java.io.File/createTempFile "records" ".log.gz"))

(defn new-compressed-stream
  []
  (let [temp-file (gen-temp-file)]
    {:file temp-file
     :stream (-> temp-file io/output-stream GZIPOutputStream.)}))

(defn epoch
  []
  (float (/ (System/currentTimeMillis) 1000)))

(defn time-to-save?
  [batch-size item-count last-write interval]
  (let [ts (epoch)
        deadline (+ last-write interval)]
    (info ts deadline last-write batch-size item-count)
    (and (> item-count 0)
      (or (>= ts deadline )
        (>= item-count batch-size)))))

(defn event-sink
  [batch-size aws-kinesis-stream interval]
  (let [item-counter (atom 0)
        last-sequence (atom nil)
        last-write (atom (epoch))
        check-stream-status (fn[]
                              (if (time-to-save? batch-size @item-counter @last-write interval )
                                (do
                                  (reset! item-counter 0)
                                  (reset! last-write (epoch))
                                  true)))]
      (let [event-processor (fn[records]
                              (info "Event processing...")

                                (db/insert-batch records)
                                (info "Found record count" @item-counter)
                                (check-stream-status))]
      event-processor)))

(defn start-worker
  [app-name aws-key aws-secret aws-endpoint aws-kinesis-stream s3-bucket batch-size interval]
  (kinesis/worker!
    :app app-name
    :stream aws-kinesis-stream
    :checkpoint false
    :credentials {:access-key aws-key :secret-key aws-secret :endpoint aws-endpoint }
    :endpoint (format "kinesis.%s.amazonaws.com" aws-endpoint)
    :processor (event-sink batch-size aws-kinesis-stream interval)))

(defrecord KinesisConsumer [app-name aws-key aws-secret aws-endpoint aws-kinesis-stream
                            s3-bucket batch-size interval cons-chan channel]
  component/Lifecycle

  (start [component]
    (try
      (warn "Starting KINESIS CONSUMER Component " aws-kinesis-stream aws-key aws-endpoint)
      (start-worker app-name aws-key aws-secret aws-endpoint
                    aws-kinesis-stream s3-bucket batch-size interval)
      (assoc component :channel (chan))
      (catch Throwable t
        (do
        (warn "[KINESIS-CONSUMER] FAILED")
        (error t)
        )))))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :default 8989
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
    ["-a" "--app-name NAME" "Application name to use for Kinesis Stream"
      :default "kinesis-sample-consumer0"]
    ["-c" "--checkpoint CHECKPOINT" "Checkpoint where the application left"
      :validate [false "Checkpoint not implemented yet!"]] ; TODO
    [nil "--aws-key KEY" "AWS KEY"]
    [nil "--aws-secret SECRET" "AWS SECRET"]
    [nil "--aws-endpoint ENDPOINT" "AWS ENDPOINT to use" :default "eu-west-1"]
    [nil "--aws-kinesis-stream STREAM" "AWS Kinesis Stream name"]
    ["-s" "--s3-bucket BUCKET" "S3 Bucket to output" :parse-fn str
      :validate [#(> (count %) 0) "S3 Bucket must be supplied"]]
    ["-b"  "--batch-size SIZE" :default (int 1e6) :parse-fn #(Integer/parseInt %)]
    ["-i" "--interval SECONDS" "Seconds between checkpoints"
      :default 180 :parse-fn #(Integer/parseInt %)]
    ["-h" "--help"]
    ])


(defn app-system
  [options]
  (let [{:keys [port aws-key aws-secret aws-endpoint aws-kinesis-stream pipe
                s3-bucket app-name batch-size interval]} options
      aws-options (select-keys options [:aws-key :aws-secret :aws-endpoint :aws-kinesis-stream])]

    (defcredential aws-key aws-secret aws-endpoint)

    (-> (component/system-map
          :pipe (map->KinesisConsumer (merge {:app-name app-name :s3-bucket s3-bucket
                                          :batch-size batch-size :interval interval }
                                          aws-options))
          :app (map->HTTP {:port port})))))

(defn -main
  [& args]
  ; (timbre/set-config! log-base/log-config )

  (let [{:keys [options summary errors]} (parse-opts args cli-options)]
    (when (:help options)
      (println summary)
      (System/exit 0))

    (when errors
      (println errors)
      (System/exit 1))

    ;remove this when the tools.cli required option works!
    (when-not (contains? options :s3-bucket)
      (println "Missing S3 Bucket setting. Please supply `--s3-bucket` option")
      (System/exit 1))

    (info "Options-> " (select-keys options [:app-name :checkpoint :aws-kinesis-stream :s3-bucket :batch-size]))

    (jmx/start JR)
    ; report to console in every 100 seconds
    (console/start CR 100)
    (let [sys (component/start (app-system options))]
      (info "System started.."))))
