(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [core :as rls]]))

(def server-options {:port 8080})

(defn post [reqs]
  (http/request
    {:url    (str
               "http://localhost:"
               (:port server-options))
     :method :post
     :body   (cheshire/generate-string reqs)}))

(def winners-per-second 10)

(defn test-rate-limited-service []
  (reify
    rls/IRateLimitedService
    (poll-size [_]
      winners-per-second)
    (request-batch [_ reqs]
      reqs)))

(defonce system (rls/make-system server-options (test-rate-limited-service) 10000))

(defn requests [n-requests n-in-request]
  (map
    (fn [i]
      (map (fn [ii] (identity [i ii])) (range n-in-request)))
    (range n-requests)))

(deftest request-test
  "Puts get through the system to request."
  (prn
    (time
      (do
        (rls/start system)
        (let [requests (requests 2000 40)
              promises (vec
                         (map
                           #(let [p (promise)]
                              (rls/start-thread
                                (str "post" (ffirst %))
                                (fn []
                                  (deliver p @(post %))))
                              p)
                           requests))
              resps (map #(cheshire/parse-string (:body @%)) promises)
              n-winners (reduce
                          (fn [a w] (+ a w))
                          0
                          (map (fn [[w _]] (count w)) resps))]
          (rls/stop system)
          (prn "n-winners" n-winners)
          (is (= (mod n-winners winners-per-second) 0)))))))

(comment
  (count (requests 1000 40))
  (cheshire/generate-string [[1] [2] [3]])
  (cheshire/parse-string "[[1], [2], [3]]")

  (rls/start system)
  (rls/running? @system)
  (rls/stop system)
  (deref system)
  (run-tests)
  )