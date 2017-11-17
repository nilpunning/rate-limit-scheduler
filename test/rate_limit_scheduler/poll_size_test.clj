(ns rate-limit-scheduler.poll-size-test
  (:require [clojure.test :refer :all]
            [rate-limit-scheduler
             [poll-size :as poll-size]])
  (:import [java.lang System]))

(deftest safe-poll-size-ms-since
  "If s-since-last-request > x-rate-limit-reset, assume a reset."
  (let [n 10]
    (is (= n
           (poll-size/safe-poll-size
             (constantly n)
             {:x-rate-limit-limit     100
              :x-rate-limit-reset     2
              :x-rate-limit-remaining 1
              ::poll-size/last-request-time
                                      (- (System/currentTimeMillis) 30000.0)}
             nil)))))
(comment
  (run-tests)
  )