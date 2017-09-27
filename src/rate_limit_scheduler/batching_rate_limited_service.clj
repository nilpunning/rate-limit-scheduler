(ns rate-limit-scheduler.batching-rate-limited-service
  (:require [rate-limit-scheduler
             [core :as rls]])
  (:import [java.lang Math]))

(def init-state
  {; rate limit per minute
   :limit     2400
   ; remaining in period
   :remaining 2
   ; seconds until period ends and remaining resets
   :reset     60})

(defn batching-rate-limited-service []
  (let [state (atom init-state)]
    (reify
      rls/IRateLimitedService
      (split-predicate [_ req]
        (:webhook req))
      (poll-size [_]
        (let [{:keys [limit remaining reset]} state]
          (if (< reset 5)
            0
            (min
              (int (Math/floor (+ (* (/ limit 2 50) (- 60 reset)) 2)))
              remaining))))
      (request-batch [_ reqs]
        (doseq [req reqs]
          )))))
