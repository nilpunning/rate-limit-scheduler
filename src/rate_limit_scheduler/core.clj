(ns rate-limit-scheduler.core
  (:require [clojure.core.async :as a]
            [rate-limit-scheduler
             [durable-queue :as dq]
             [split-queue :as sq]])
  (:import [java.io Closeable]))

(defprotocol IRateLimitedService
  (split-predicate [this req]
    "Defines how to split request objects.")
  (request [this req]
    "Makes request and returns [[accepted] [rejected] {:limit :remaining :reset}].")
  (request-batch [this reqs]
    "Takes batches of requests and returns [[accepted] [rejected]]")
  (on-success [this responses]
    "Called with successfully made responses.")
  (on-rejected [this reqs]
    "Called with rejected requests."))

(defn put [rate-limit-scheduler req]
  (let [{:keys [durable-queue]} rate-limit-scheduler]
    (dq/put! durable-queue req)))

(defn split-loop [rate-limit-scheduler]
  "Reads from durable queue writes to split queue."
  (let [{:keys [split-loop-command-chan
                timeout
                durable-queue
                split-queue
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (let [cmd (a/poll! split-loop-command-chan)]
        (when (not= cmd :stop)
          (let [req (dq/take! durable-queue)]
            (when (not= req :timeout)
              (when (dosync
                      (let [[able? new-sq]
                            (sq/put
                              @split-queue
                              (split-predicate rate-limited-service @req)
                              req)]
                        (when able?
                          (ref-set split-queue new-sq))
                        (not able?)))
                ; When not able to put onto split queue back off.
                (Thread/sleep timeout)))
            (recur)))))))

(defn request-loop [rate-limit-scheduler]
  "Reads from the split-queue and makes requests."
  (let [{:keys [request-loop-command-chan
                timeout
                durable-queue
                split-queue
                requesting?
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (let [cmd (a/poll! request-loop-command-chan)]
        (when (not= cmd :stop)
          (let [[able? req] (dosync
                              (let [[able? req new-sq] (sq/poll @split-queue)]
                                (when able?
                                  (ref-set split-queue new-sq))
                                [able? req]))]
            (if able?
              (do
                (reset! requesting? true)
                (request rate-limited-service @req)
                (dq/complete! req)
                (reset! requesting? false))
              ; When not able to poll from split-queue back off.
              (Thread/sleep timeout)))
          (recur))))))

(defn start-thread [name fn]
  (doto (Thread. ^Runnable fn)
    (.setName name)
    (.start)))

(defn start [rate-limit-scheduler]
  (start-thread "split-loop " (partial split-loop rate-limit-scheduler))
  (start-thread "request-loop" (partial request-loop rate-limit-scheduler)))

(defn stop [rate-limit-scheduler]
  (let [{:keys [command-chan]} rate-limit-scheduler]
    (a/put! command-chan :stop)))

(defn drain [rate-limit-scheduler]
  (let [{:keys [split-loop-command-chan
                request-loop-command-chan
                split-queue
                requesting?]}
        rate-limit-scheduler]
    (a/put! split-loop-command-chan :stop)
    (while (and (> (get @split-queue ::sq/n) 0)
                (not @requesting?))
      (Thread/sleep 100))
    (a/put! request-loop-command-chan :stop)))

(defn delete! [rate-limit-scheduler]
  (let [{:keys [durable-queue]} rate-limit-scheduler]
    (dq/delete! durable-queue)))

(defn rate-limit-scheduler
  [rate-limited-service timeout limit init-round-robin]
  (let [command-chan (a/chan)
        split-loop-command-chan (a/chan)
        request-loop-command-chan (a/chan)
        mult (a/mult command-chan)]

    (a/tap mult split-loop-command-chan)
    (a/tap mult request-loop-command-chan)

    {:rate-limited-service      rate-limited-service
     :timeout                   timeout
     :durable-queue             (dq/make! timeout)
     :command-chan              command-chan
     :split-loop-command-chan   split-loop-command-chan
     :request-loop-command-chan request-loop-command-chan
     :split-queue               (ref (sq/make limit init-round-robin))
     :requesting?               (atom false)}))
