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

(def sample-rate-limited-service
  (reify
    IRateLimitedService
    (split-predicate [_ req]
      (:id req))
    (request [_ req]
      (println "request" req)
      [[{:answer true}] [] {:limit 100 :remaining 100 :reset 100}])
    (request-batch [_ reqs]
      (println "request-batch" reqs)
      [(map (constantly {:answer true}) reqs) [] {:limit 100 :remaining 100 :reset 100}])
    (on-success [_ responses]
      (println "on-success" responses))
    (on-rejected [_ reqs]
      (println "on-rejected" reqs))))

(defn put [rate-limit-scheduler request]
  (let [{:keys [durable-queue]} rate-limit-scheduler]
    (dq/put! durable-queue request)))

(defn split-loop [rate-limit-scheduler]
  "Reads from durable queue writes to split queue."
  (let [{:keys [timeout
                durable-queue
                command-chan
                split-queue
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (let [cmd (a/poll! command-chan)]
        (when (not= cmd :stop)
          (let [request (dq/take! durable-queue)]
            (when (not= request :timeout)
              (when (dosync
                      (let [[able? new-sq]
                            (sq/put
                              @split-queue
                              (split-predicate rate-limited-service request)
                              request)]
                        (ref-set split-queue new-sq)
                        (not able?)))
                ; When not able to put onto split queue back off.
                (Thread/sleep timeout)))
            (recur)))))))

(defn start [rate-limit-scheduler]
  (doto (Thread. ^Runnable (partial split-loop rate-limit-scheduler))
    (.setName "split-loop")
    (.start)))

(defn stop [rate-limit-scheduler]
  (let [{:keys [command-chan]} rate-limit-scheduler]
    (a/put! command-chan :stop)))

(defn delete! [rate-limit-scheduler]
  (let [{:keys [durable-queue]} rate-limit-scheduler]
    (dq/delete! durable-queue)))

(defn rate-limit-scheduler [timeout limit init-round-robin]
  {:timeout              timeout
   :rate-limited-service sample-rate-limited-service
   :durable-queue        (dq/make! timeout)
   :command-chan         (a/chan)
   :split-queue          (ref (sq/make limit init-round-robin))})

(comment

  (def rls (rate-limit-scheduler))
  (start rls)
  (put rls {:id 0 :content :stuff})
  (put rls {:id 1 :content :junk})
  (put rls {:id 1 :content :more-junk})
  (stop rls)

  (delete! rls)

  )
