(ns rate-limit-scheduler.core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
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
                    (map #(identity {::body % ::channel channel})))
          status (dosync
                   (if (true? (::collecting? @system))
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

(defn collecting-to-draining [system]
  (let [{:keys [::collecting-queue]} system
        {:keys [::sq/limit ::sq/last-taken]} collecting-queue]
    (assoc
      system
      ::draining-queue collecting-queue
      ::collecting-queue (sq/make limit last-taken))))

(defn request-loop [system]
  (let [{:keys [::service ::command-chan]} @system]
    (loop [start-time (System/currentTimeMillis)]
      (when (not= (a/poll! command-chan) ::stop)
        (dosync (alter system collecting-to-draining))
        (let [n (poll-size service)
              {:keys [::draining-queue]} @system
              [winners loser-queue] (sq/poll draining-queue n)
              [losers _] (sq/drain draining-queue)
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
                          [(map #(::body %) (get winner-groups channel))
                           (map #(::body %) (get loser-groups channel))])}))
          (let [end-time (System/currentTimeMillis)
                diff (- end-time start-time)]
            (when (< diff 2000)
              (Thread/sleep (- 2000 diff)))
            (recur end-time)))))))

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
         ::command-chan     (a/chan)
         ::collecting?      false
         ::collecting-queue (sq/make limit)
         ::draining-queue   (sq/make limit)}))
    system))

(defn start [system]
  (dosync
    (alter
      system
      merge
      {::server              (run-server (::server-options @system) system)
       ::request-loop-thread (start-thread
                               "request-loop"
                               (partial request-loop system))
       ::collecting?         true})))

(defn stop [system]
  (dosync (alter system assoc ::collecting? false))
  (let [{:keys [::server ::command-chan ::request-loop-thread]} @system]
    (a/put! command-chan ::stop)
    (.join ^Thread request-loop-thread)
    (server :timeout 0)))
