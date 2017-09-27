(ns rate-limit-scheduler.split-queue-test
  (:require [clojure.test :refer :all]
            [rate-limit-scheduler.split-queue :as split-queue]))

(deftest limit
  "Limit number of total elements in split queue."
  (let [sq0 (split-queue/make 3 Long/MIN_VALUE)
        [able0? sq1] (split-queue/put sq0 0 "a")
        [able1? sq2] (split-queue/put sq1 0 "b")
        [able2? sq3] (split-queue/put sq2 1 "c")
        [able3? sq4] (split-queue/put sq3 1 "d")]
    (is (= able0? true))
    (is (= able1? true))
    (is (= able2? true))
    (is (= able3? false))))

(deftest ordered-queue
  "Items are taken from single queue in first in first out order."
  (let [sq0 (split-queue/make 3 Long/MIN_VALUE)
        [_ sq1] (split-queue/put sq0 0 "a")
        [_ sq2] (split-queue/put sq1 0 "b")
        [_ sq3] (split-queue/put sq2 0 "c")
        [_ a sq4] (split-queue/poll sq3)
        [_ b sq5] (split-queue/poll sq4)
        [_ c sq6] (split-queue/poll sq5)
        [_ d sq7] (split-queue/poll sq6)]
    (is (= ["a" "b" "c" nil] [a b c d]))))

(deftest round-robin-queues
  "Queues should be chosen from in a round robin fashion."
  (let [sq0 (split-queue/make 3 Long/MIN_VALUE)
        [_ sq1] (split-queue/put sq0 0 "a")
        [_ sq2] (split-queue/put sq1 0 "b")
        [_ sq3] (split-queue/put sq2 1 "c")
        [_ a sq4] (split-queue/poll sq3)
        [_ c sq5] (split-queue/poll sq4)
        [_ b sq6] (split-queue/poll sq5)
        [_ d sq7] (split-queue/poll sq6)]
    (is (= ["a" "c" "b" nil] [a c b d]))))

(deftest poll-batch
  "Test poll batch."
  (let [sq0 (split-queue/make 3 Long/MIN_VALUE)
        [able0? sq1] (split-queue/put sq0 0 "a")
        [able1? sq2] (split-queue/put sq1 0 "b")
        [able2? sq3] (split-queue/put sq2 1 "c")
        [able3? sq4] (split-queue/put sq3 1 "d")
        [vals sq5] (split-queue/poll sq4 5)]
    (is (= vals ["a" "c" "b"]))))

(comment
  (run-tests)
  )
