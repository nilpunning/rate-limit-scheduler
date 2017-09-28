(ns rate-limit-scheduler.core
  (:require [clojure.core.async :as a]
            [rate-limit-scheduler
             [durable-queue :as dq]
             [split-queue :as sq]]))

(defprotocol IRateLimitedService
  (split-predicate [this req]
    "Defines how to split request objects.")
  (poll-size [this]
    "Number of requests to batch.")
  (request-batch [this reqs]
    "Makes the requests."))

(defn put [rate-limit-scheduler req]
  (let [{:keys [durable-queue]} rate-limit-scheduler]
    (dq/put! durable-queue req)))

(defn split-loop [rate-limit-scheduler]
  "Reads from durable queue writes to split queue."
  (let [{:keys [split-loop-command-chan
                timeout
                durable-queue
                state
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (when (not= (a/poll! split-loop-command-chan) :stop)
        (let [req (dq/take! durable-queue)]
          (when (not= req :timeout)
            (when (dosync
                    (let [[able? new-sq]
                          (sq/put
                            (:split-queue @state)
                            (split-predicate rate-limited-service @req)
                            req)]
                      (when able?
                        (alter state assoc :split-queue new-sq))
                      (not able?)))
              ; When not able to put onto split queue back off.
              (Thread/sleep timeout)))
          (recur))))))

(defn request-loop [rate-limit-scheduler]
  "Reads from the split-queue and makes requests."
  (let [{:keys [request-loop-command-chan
                timeout
                durable-queue
                state
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (when (not= (a/poll! request-loop-command-chan) :stop)
        (let [n (poll-size rate-limited-service)
              reqs (dosync
                     (let [[reqs new-sq] (sq/poll (:split-queue @state) n)]
                       (when (> (count reqs) 0)
                         (alter state assoc :split-queue new-sq))
                       reqs))]
          (when (> (count reqs) 0)
            (request-batch rate-limited-service (map (fn [r] @r) reqs))
            (doseq [r reqs]
              (dq/complete! r)))
          ; When not able to poll from split-queue back off.
          (when (< (count reqs) n)
            (Thread/sleep timeout)))
        (recur)))))

(defn metrics-loop [rate-limit-scheduler]
  "Log metrics every second."
  (let [{:keys [metrics-loop-command-chan
                durable-queue
                state]}
        rate-limit-scheduler]
    (loop []
      (when (not= (a/poll! metrics-loop-command-chan) :stop)
        (let [s {:durable-queue (dq/stats durable-queue)
                 :split-queue   (sq/stats (:split-queue @state))}]
          (println s))
        (Thread/sleep 1000)
        (recur)))))

(defn running? [state]
  (->> [:split-loop-thread
        :request-loop-thread
        :metrics-loop-thread]
       (select-keys state)
       vals
       (map #(.isAlive ^Thread %))
       (some true?)
       boolean))

(defn start-thread [name fn]
  (doto (Thread. ^Runnable fn)
    (.setName name)
    (.start)))

(defn start-if-not-already-started [state rate-limit-scheduler]
  (if (running? state)
    state
    (merge
      state
      (->> {:split-loop-thread   split-loop
            :request-loop-thread request-loop
            :metrics-loop-thread metrics-loop}
           (map (fn [[k fn]]
                  [k (start-thread
                       (name k)
                       #(fn rate-limit-scheduler))]))
           (into {})))))

(defn start [rate-limit-scheduler]
  (dosync
    (alter
      (:state rate-limit-scheduler)
      start-if-not-already-started
      rate-limit-scheduler)))

(defn drain [rate-limit-scheduler]
  (let [{:keys [split-loop-command-chan
                request-loop-command-chan
                metrics-loop-command-chan
                state
                durable-queue]}
        rate-limit-scheduler]
    (when (running? @state)
      (a/put! split-loop-command-chan :stop)
      (.join ^Thread (:split-loop-thread @state))
      (while (dq/in-progress? durable-queue)
        (Thread/sleep 100))
      (a/put! request-loop-command-chan :stop)
      (.join ^Thread (:request-loop-thread @state))
      (a/put! metrics-loop-command-chan :stop)
      (.join ^Thread (:metrics-loop-thread @state)))))

(defn rate-limit-scheduler
  [rate-limited-service timeout limit init-round-robin]
  {:rate-limited-service      rate-limited-service
   :timeout                   timeout
   :durable-queue             (dq/make! timeout)
   :split-loop-command-chan   (a/chan)
   :request-loop-command-chan (a/chan)
   :metrics-loop-command-chan (a/chan)
   :state                     (ref {:split-queue         (sq/make
                                                           limit
                                                           init-round-robin)
                                    :split-loop-thread   (Thread.)
                                    :request-loop-thread (Thread.)
                                    :metrics-loop-thread (Thread.)})})
