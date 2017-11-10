(ns rate-limit-scheduler.poll-size
  (:require [rate-limit-scheduler
             [core :as rls]])
  (:import [java.lang Math System]))

(defn linear-poll-size
  "
  As the limit is approached more requests are made in an attempt to get as
  close to the limit as possible.  If a request is made every two seconds, it
  will blow through rate in 18 seconds.  But will probably take much longer
  when time to make requests are taken into account.
  "
  [limit reset remaining]
  (int (Math/round ^double (/ (* (/ limit 60) (- 65 reset)) 2))))

(comment
  (let [i (range 0 61 2)
        p (map #(linear-poll-size 2400 (- 60 %) 60) i)
        r (reductions + p)]
    (prn "Time, Poll Size, Total Requests")
    (doseq [t (partition 3 (interleave i p r))]
      (prn t)))
  )

(defn safe-poll-size
  "
  Avoids polling last five seconds to avoid requests bleeding between periods
  and will not return greater than the remaining limit.
  specific-poll-size-fn         ; (fn [limit reset remaining]) => int
  request-state-defaults        ; same format as request-state
  request-state                 ; map of the following format:
  {:x-rate-limit-limit     int  ; rate limit per minute
   :x-rate-limit-reset     int  ; seconds until period ends and remaining resets
   :x-rate-limit-remaining int} ; remaining in period
  "
  [specific-poll-size-fn
   request-state-defaults
   request-state
   ms-since-last-request]
  (let [{:keys [x-rate-limit-limit
                x-rate-limit-reset
                x-rate-limit-remaining]}
        (merge
          {:x-rate-limit-limit     100
           :x-rate-limit-reset     60
           :x-rate-limit-remaining 1}
          request-state-defaults
          request-state)
        s-since-last-request (/ ms-since-last-request 1000.0)
        expired (> s-since-last-request x-rate-limit-reset)
        remaining (if expired x-rate-limit-limit x-rate-limit-remaining)
        reset (if expired 60 x-rate-limit-reset)]
    (if (< reset 5)
      0
      (min
        (specific-poll-size-fn
          x-rate-limit-limit
          (Math/round ^double (- x-rate-limit-reset s-since-last-request))
          remaining)
        remaining))))