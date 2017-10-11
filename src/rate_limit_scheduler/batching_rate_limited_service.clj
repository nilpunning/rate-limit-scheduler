(ns rate-limit-scheduler.batching-rate-limited-service
  (:require [rate-limit-scheduler
             [core :as rls]])
  (:import [java.lang Math System]))

(def init-state
  {; rate limit per minute
   :limit     2400
   ; seconds until period ends and remaining resets
   :reset     60
   ; remaining in period
   :remaining 1

   :last-updated})

(defn linear-poll-size
  [limit reset remaining]
  ; As the limit is approached more requests are made in an attempt
  ; to get as close to the limit as possible.
  (int (Math/round ^double (/ (* (/ limit 60) (- 65 reset)) 2))))

(comment
  (doseq [i (range 61)]
    (println i (linear-poll-size 60 (- 60 i) 60)))

  (System/currentTimeMillis)
  )

(defn batching-rate-limited-service []
  (let [state (atom init-state)]
    (reify
      rls/IRateLimitedService
      (poll-size [_]
        (let [{:keys [limit reset remaining]} state]
          (if (< reset 5)
            0
            (min
              (linear-poll-size limit reset remaining)
              remaining))))
      (request-batch [_ reqs]

        (doseq [req reqs]
          )))))


