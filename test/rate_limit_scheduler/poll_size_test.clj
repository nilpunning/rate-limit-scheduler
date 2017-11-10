(ns rate-limit-scheduler.poll-size-test
  (:require [clojure.test :refer :all]
            [rate-limit-scheduler
             [poll-size :as poll-size]]))

(deftest safe-poll-size-ms-since
  "If s-since-last-request > x-rate-limit-reset, assume a reset."
  (let [n 10]
    (is (= n
           (poll-size/safe-poll-size
             (constantly n)
             {:x-rate-limit-limit     100
              :x-rate-limit-reset     2
              :x-rate-limit-remaining 1}
             nil
             30000.0)))))
(comment
  (run-tests)
  )