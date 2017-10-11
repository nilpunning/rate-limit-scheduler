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

(defn put [system channel reqs]
  (update
    system
    ::collecting-queue
    (fn [fq]
      (->> reqs
           (map #(identity {::request % ::channel channel}))
           (sq/put fq)))))

(defn do-handle [req system channel]
  (server/on-close
    channel
    (fn [status]
      (println "channel closed" status)))

  (->> (:body req)
       io/reader
       cheshire/parse-stream
       (map (fn [r] {::request r ::channel channel}))
       (swap! system put channel)))

(defn server-handler [system req]
  (server/with-channel req channel
    (do-handle req system channel)))

(defn run-server [server-options system]
  (server/run-server
    (partial #'server-handler system)
    server-options))

(defn collecting-to-draining [system]
  (let [{:keys [::collecting-queue]} system
        {:keys [::sq/limit ::sq/last-taken]} collecting-queue]
    (-> system
        (assoc ::draining-queue collecting-queue)
        (assoc ::collecting-queue (sq/make limit last-taken)))))

(defn remove-channel [req]
  (map #(dissoc % ::channel) req))

(defn request-loop [system]
  (let [{:keys [::service ::command-chan ::collecting-queue ::draining-queue]}
        system]
    (loop [start-time (System/currentTimeMillis)]
      (when (not= (a/poll! command-chan) :stop)
        (swap! system collecting-to-draining)
        (let [n (poll-size service)
              {:keys [::draining-queue]} @system
              [winners loser-queue] (sq/poll draining-queue)
              [losers _] (sq/drain draining-queue)
              winning-resps (request-batch service winners)
              winner-groups (group-by ::channel winners)
              loser-groups (group-by ::channel losers)
              channels (set (concat (keys winner-groups) (keys loser-groups)))]
          (doseq [channel channels]
            (server/send!
              channel
              {:status  200
               :headers {"Content-Type" "application/json"}
               :body    (cheshire/generate-string
                          [(remove-channel (get winner-groups channel))
                           (remove-channel (get loser-groups channel))])}))

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
  (let [system (atom {})]
    (reset!
      system
      {::server           (run-server server-options system)
       ::service          rate-limited-service
       ::command-chan     (a/chan)
       ::collecting-queue (sq/make limit 0)
       ::draining-queue   (sq/make limit 0)})
    ;(start-thread "request-loop" (partial request-loop system))
    system))

(defn stop [system]
  ((::server @system) :timeout 0))

(defn drain [system]
  (let [{:keys [::collecting-queue ::draining-queue ::command-chan]} system]
    ))