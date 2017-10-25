(ns rate-limit-scheduler.core
  (:require [clojure.java.io :as io]
            [org.httpkit.server :as server]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [split-queue :as sq]])
  (:import [java.lang System Thread]))

(defprotocol IRateLimitedService
  (poll-size [this]
    "Number of requests to batch.")
  (request-batch [this reqs]
    "Makes the requests."))

(defn server-handler [system req]
  (server/with-channel req channel
    (let [reqs (->> req
                    :body
                    io/reader
                    cheshire/parse-stream
                    (map #(identity {::request % ::channel channel})))
          status (dosync
                   (if (::collecting? @system)
                     (if (sq/able? (::collecting-queue @system))
                       (do
                         (alter system update ::collecting-queue sq/put reqs)
                         false)
                       ; Too Many Requests
                       429)
                     ; Service Unavailable
                     503))]
      (when status
        (server/send! channel {:status status})))))

(defn run-server [server-options system]
  (server/run-server
    (partial #'server-handler system)
    server-options))

(defn reset-collecting-queue [system]
  (dosync
    (let [{:keys [::collecting-queue]} @system
          {:keys [::sq/limit ::sq/last-taken]} collecting-queue]
      (alter
        system
        assoc
        ::collecting-queue
        (sq/make limit last-taken))
      collecting-queue)))

(defn request-loop [system]
  (loop [start-time (System/currentTimeMillis)]
    (when (::collecting? @system)
      (let [draining-queue (reset-collecting-queue system)
            {:keys [::service]} @system
            n (poll-size service)
            [winners loser-queue] (sq/poll draining-queue n)
            [losers _] (sq/drain loser-queue)
            winner-resps (request-batch service winners)
            winner-groups (group-by ::channel winner-resps)
            loser-groups (group-by ::channel losers)
            channels (set (concat (keys winner-groups) (keys loser-groups)))]
        (doseq [channel channels]
          (server/send!
            channel
            {:status  200
             :headers {"Content-Type" "application/json"}
             :body    (cheshire/generate-string
                        [(map
                           #(dissoc % ::channel)
                           (get winner-groups channel))
                         (map
                           #(dissoc % ::channel)
                           (get loser-groups channel))])}))
        (let [end-time (System/currentTimeMillis)
              diff (- end-time start-time)]
          (when (< diff 2000)
            (Thread/sleep (- 2000 diff)))
          (recur end-time))))))

(defn start-thread [name fn]
  (doto (Thread. ^Runnable fn)
    (.setName name)
    (.start)))

(defn make-system [server-options rate-limited-service limit]
  (let [system (ref {})]
    (dosync
      (ref-set
        system
        {::server-options   server-options
         ::service          rate-limited-service
         ::collecting?      false
         ::running?         false
         ::collecting-queue (sq/make limit)}))
    system))

(defn start [system]
  (when (not (::running? @system))
    (dosync (alter system assoc ::collecting? true ::running? true))
    (let [server (run-server (::server-options @system) system)
          thread (start-thread "request-loop" (partial request-loop system))]
      (dosync
        (alter
          system
          assoc
          ::server server
          ::request-loop-thread thread)))))

(defn stop [system]
  (when (::running? @system)
    (dosync (alter system assoc ::collecting? false))
    (let [{:keys [::server ::request-loop-thread]} @system]
      (.join ^Thread request-loop-thread)
      (server :timeout 60)
      (dosync
        (alter system dissoc ::server ::request-loop-thread)
        (alter system assoc ::running? false)))))
