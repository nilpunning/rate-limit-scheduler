(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [rate-limit-scheduler
             [durable-queue :as dq]
             [core :as rls]]))

(defn test-rate-limited-service [request-calls]
  (reify
    rls/IRateLimitedService
    (split-predicate [_ req]
      (:id req))
    (request [_ req]
      (swap! request-calls inc)
      [[{:answer true}] [] {:limit 100 :remaining 100 :reset 100}])
    (request-batch [_ reqs]
      (println "request-batch" reqs)
      [(map (constantly {:answer true}) reqs) [] {:limit 100 :remaining 100 :reset 100}])
    (on-success [_ responses]
      (println "on-success" responses))
    (on-rejected [_ reqs]
      (println "on-rejected" reqs))))

(defn cleanup! []
  (dq/delete! (dq/make! 10)))

(deftest request-test
  "Puts get through the system to request."
  (cleanup!)
  (let [n 10000
        request-calls (atom 0)
        service (test-rate-limited-service request-calls)
        scheduler (rls/rate-limit-scheduler service 1000 n 0)]
    (rls/start scheduler)
    (doseq [x (range n)]
      (rls/put scheduler {:id x}))
    (rls/drain scheduler)
    (rls/delete! scheduler)
    (is (= n @request-calls))))

(comment
  (run-tests)
  )
