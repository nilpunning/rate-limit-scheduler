(ns rate-limit-scheduler.core
  (:require [clojure.core.async :as a]
            [durable-queue :as dq])
  (:import [java.io Closeable]))


(defprotocol IRateLimitedService
  (split-predicate [req]
    "Defines how to split request objects.")
  (request [req]
    "Makes request and returns [[accepted] [rejected] {:limit :remaining :reset}].")
  (request-batch [reqs]
    "Takes batches of requests and returns [[accepted] [rejected]]")
  (on-success [responses]
    "Called with successfully made responses.")
  (on-rejected [reqs]
    "Called with rejected requests."))


(def sample-rate-limited-service
  (reify
    IRateLimitedService
    (split-predicate [req]
      (:id req))
    (request [req]
      (println "request" req)
      [[{:answer true}] [] {:limit 100 :remaining 100 :reset 100}])
    (request-batch [reqs]
      (println "request-batch" reqs)
      [(map (constantly {:answer true}) reqs) [] {:limit 100 :remaining 100 :reset 100}])
    (on-success [responses]
      (println "on-success" responses))
    (on-rejected [reqs]
      (println "on-rejected" reqs))))

(defn put [rate-limit-scheduler request]
  (let [{:keys [durable-queues
                period]}
        rate-limit-scheduler]
    (dq/put! durable-queues :ingress request period)))

(defn main-loop [rate-limit-scheduler]
  (let [{:keys [durable-queues
                command-chan
                period
                ingress-buffer
                rate-limited-service]}
        rate-limit-scheduler]
    (loop []
      (let [cmd (a/poll! command-chan)]
        (when (not= cmd :stop)
          (let [request (dq/take! durable-queues :ingress period :timeout)]
            (if (= request :timeout)
              (recur)
              (do
                (a/put! ingress-buffer request)
                (recur)))))))))

(defn splitter [ingress-buffer rate-limited-service split-queues]
  (a/go-loop []
    (let [request (a/<! ingress-buffer)]
      (println request)
      (dq/complete! request))
    (recur)))

(defn start [rate-limit-scheduler]
  (doto (Thread. ^Runnable (partial main-loop rate-limit-scheduler))
    (.setName "main-loop")
    (.start)))

(defn stop [rate-limit-scheduler]
  (let [{:keys [command-chan]}
        rate-limit-scheduler]
    (a/put! command-chan :stop)))

(defn delete! [rate-limit-scheduler]
  (let [{:keys [durable-queues]}
        rate-limit-scheduler]
    (dq/delete! durable-queues)))

(defn rate-limit-scheduler []
  (let [ingress-buffer (a/chan 6000)
        split-queues (atom {})
        splitter (splitter
                   ingress-buffer
                   sample-rate-limited-service
                   split-queues)]
    {:rate-limited-service sample-rate-limited-service
     :durable-queues       (dq/queues "./data" {})
     :command-chan         (a/chan)
     :period               10000
     :ingress-buffer       ingress-buffer
     :splitter             splitter

     ;:accepted-chan        (a/chan 6000)
     ;:rejected-chan        (a/chan 6000)
     ;:split-queues         {}
     ;
     ;:limit                nil
     ;:remaining            nil
     ;:reset                nil
     }))

(comment

  (def rls (rate-limit-scheduler))
  (start rls)
  (put rls {:id 0 :content :stuff})
  (put rls {:id 1 :content :junk})
  (put rls {:id 1 :content :more-junk})
  (stop rls)

  (delete! rls)

  )
