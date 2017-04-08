(ns kinesis2.db
  (:require [clojure.java.jdbc :as jdbc]
    [com.stuartsierra.component :as component]
    [honeysql.core :as sql]
    [honeysql.helpers :refer :all]
    [taoensso.timbre :as log]
    [cheshire.core :refer :all]
    [taoensso.carmine :as redis]
    [kinesis2.utils :refer [in? epoch->datetime <urlsafe-str]])
  (:import [org.postgresql.util PGobject]))

(def DEFAULT-DB-SPEC
{:classname   "org.postgresql.Driver"
 :subprotocol "postgresql"
 :subname "events"
 :user     "pgusr"
 :password "pgusr"
 :host "127.0.0.1"
 :db "events"})


(defrecord Database [host port connection]
;; Implement the Lifecycle protocol
component/Lifecycle

(start [component]
  (println ";; Starting database")

  (let [conn nil]

    (assoc component :connection conn)))

(stop [component]
  (println ";; Stopping database")
   (assoc component :connection nil)))

(defn new-database [host port]
(map->Database {:host host :port port}))

(def pg-db DEFAULT-DB-SPEC)

(def insert-queue (java.util.ArrayList. 10000))

(defn pg-dt [value]
(doto (PGobject.)
  (.setType "timestamp")
  (.setValue value)))

(defn pg-json [value]
  (doto (PGobject.)
    (.setType "json")
    (.setValue value)))


(defmacro with-safe
  [& body]
  `(try
    ~@body
    (catch org.postgresql.util.PSQLException psqlex#
      (log/warn (.getSQLState psqlex#) (.getMessage psqlex#))
    )
  (catch Throwable t#
    (do (log/warn (.getMessage t#))
      nil))))

(defn query!
[db q]
  (with-safe
    (jdbc/query db q)))

(defn find-ids
[ids]
(map :id (query! pg-db
    (-> (select :id)
       (from :events)
       (where [:in :id ids])
       (limit 1e4)
       (sql/build)
       (sql/format :quoting :ansi)))))


(defmulti purify (fn [action dataset] action))

(defmethod purify :update
 [m dataset]
 (throw (java.lang.UnsupportedOperationException. "Purify UPDATE not Implemented")))

(defmethod purify :delete
 [m dataset]
 (let [ids (set (map #(get % :id) dataset))]
  (jdbc/execute! pg-db
    (-> (delete-from :events)
        (where [:in :id ids])
        (sql/build)
        (sql/format :quoting :ansi)
        ))
 ))


(defmethod purify :diff
 [m dataset]
 (let [ids (set (map #(get % :id) dataset))
       existing-ids (set (or (find-ids ids) []))
       new-ids (clojure.set/difference ids existing-ids)]
     (log/sometimes 0.01 (log/warn "New IDS: " (count new-ids) (vec (take 3 new-ids))))
     (mapv identity (filter #(and (in? new-ids (get % "id"))
              true;  (= "pv" (get % "evt_name")
               )
        dataset))))



(defn flush-events!
  [dataset]
  (when-not (empty? dataset)
    (let [
          ; _ (purify :delete dataset)
          dataset* dataset] ; (purify :diff dataset)]
      (try
          ; (log/info "Inserting dataset" dataset*)
          (when-not (empty? dataset*)
            (apply (partial jdbc/insert! pg-db :events) dataset*))
        (catch org.postgresql.util.PSQLException psqlex
          (do
            (let [sqlState (.getSQLState psqlex)]
              (log/warn sqlState (.getMessage psqlex))
              (when (= sqlState "23505")
                (map (fn[x] (with-safe
                          (jdbc/insert! pg-db :events x)))
                  dataset*)))))
        (catch Throwable t
          (do
            (log/error t)
            (throw t)))))))

(defn- parse-cookies
  "Returns a map of cookies when given the Set-Cookie string sent
  by a server."
  [#^String cookie-string]
  (when cookie-string
    (into {}
      (for [#^String cookie (.split cookie-string ";")]
        (let [keyval (map (fn [#^String x] (.trim x)) (.split cookie "=" 2))]
          [(first keyval) (<urlsafe-str (second keyval))])))))


(defn post-event
  [data]
  (let [{:keys [m epoch ip time ua params headers host srv uri body refer]} data
       request (parse-string body true)
       {:keys [args path method env headers referrer id url t form user]} request
       {:keys [X-Forward-For User-Agent Host Cookie]} headers
       cookie-data (parse-cookies Cookie)]
     {:received_at (epoch->datetime epoch)
      :ts  (epoch->datetime (str t))
      :path path :url url
      :user_data user
      :referrer referrer
      :cookies cookie-data
      :ip X-Forward-For
      :args args
      :form form
      :user_agent User-Agent}
       )
  )

(defn get-event
  [data]
  (let [{:keys [m epoch ip time ua params headers host srv uri body refer]} data
        ; _ (log/info headers)
         {:keys [x-forwarded-for Host cookie]} headers
         cookie-data (parse-cookies cookie)
         {:keys [_ref _ts _e url]} params
         is-pixel (= uri "/pixel.gif")]
    {:received_at (epoch->datetime _ts)
     :ts time
    ;  :path path
     :url url
     :ip (-> (or x-forwarded-for "") (.split ",") first)
     :evt_type (if is-pixel "pv" _e)
     :cookies cookie-data
     :referrer _ref
     :user_agent ua
     :args params}
    ))

(def pgjsonize (comp pg-json generate-string))

(def pg-mapping {
    :ts pg-dt
    :received_at pg-dt
    :cookies pgjsonize
    :args pgjsonize
    :form pgjsonize
    :user_data pgjsonize
    :orig_data pgjsonize
    })

(defn parse-data
  [sid data]
  (let [data (parse-string data true)
        {:keys [m epoch ip time ua params headers host srv uri body refer]} data
        data-map (if (= m "get")
                    (get-event data)
                    (post-event data))
        data-map (merge data-map {:id sid
                                  :orig_data  data})]
  ; (flush-events! [data-map])
  data-map
  ))

(defn transform-to-pg-objects
  [recs]
  (mapv (fn [rec]
    (apply merge
      (map (fn[[x y]]
        (if (contains?  pg-mapping x)
          {x ((get pg-mapping x) y)}
          {x y}))
        rec)))
    recs
    ))


(def server1-conn {:pool {} :spec { :host "127.0.0.1" :port 6379 }})

(defn insert-batch
  [batch]
  (doseq [row batch]
    (let [{:keys [sequence-number data partition]} row]

      (.add insert-queue (parse-data sequence-number data))))
  (log/info "Inserting events" (count insert-queue))
  (flush-events! (transform-to-pg-objects insert-queue))
  (mapv #(redis/wcar server1-conn
      (redis/publish  "foobar" (generate-string %))) insert-queue)
  (.clear insert-queue)
  )
