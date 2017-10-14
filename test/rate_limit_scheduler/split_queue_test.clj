(ns rate-limit-scheduler.split-queue-test
  (:require [clojure.test :refer :all]
            [rate-limit-scheduler.split-queue :as sq]))

(deftest limit
  "Limit number split queues."
  (let [[vals _]
        (-> (sq/make 1)
            (sq/put [:a])
            (sq/put [:b])
            (sq/poll 2))]
    (is (= vals [:a]))))

(deftest ordered-queue
  "Items are taken from single queue in first in first out order."
  (let [[vals _]
        (-> (sq/make 3)
            (sq/put [:a :b :c])
            (sq/poll 3))]
    (is (= vals [:a :b :c]))))

(deftest round-robin-queues
  "Queues should be chosen from in a round robin fashion."
  (let [[vals _]
        (-> (sq/make 3)
            (sq/put [:a :b])
            (sq/put [:c])
            (sq/poll 3))]
    (is (= vals [:a :c :b]))))

(deftest drain
  "Drain queue"
  (let [[vals _]
        (-> (sq/make 2)
            (sq/put [1 3])
            (sq/put [2])
            (sq/drain))]
    (is (= vals [1 2 3]))))

(deftest poll-empty
  "Polls of empty queue should return consistent results."
  (let [[vals _]
        (-> (sq/make 0)
            (sq/poll 1))]
    (is (= vals []))))

(comment
  (run-tests)
  )