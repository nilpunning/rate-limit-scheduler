(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [core :as rls]]))


(defn make []
  (rls/make-system {::rls/limit 10000}))

(defonce system (atom (make)))

(defn post [reqs]
  (http/request
    {:url    (str
               "http://localhost:"
               (get-in @@system [::rls/server-options :port]))
     :method :post
     :body   (cheshire/generate-string reqs)}))

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
        (rls/start @system)
        (let [requests (requests 2048 40)
              posts (vec (map post requests))
              resps (map #(cheshire/parse-string (:body @%)) posts)
              n-winners (reduce + 0 (map (fn [[w _]] (count w)) resps))]
          (rls/stop @system)
          (prn "n-winners" n-winners)
          (is (= (mod n-winners 10) 0)))))))

(comment
  (count (requests 1000 40))
  (cheshire/generate-string [[1] [2] [3]])
  (cheshire/parse-string "[[1], [2], [3]]")

  (reset! system (make))

  (rls/start @system)
  (rls/stop @system)
  (deref @system)
  (run-tests)
  )