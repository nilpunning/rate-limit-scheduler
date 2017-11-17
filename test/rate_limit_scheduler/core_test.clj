(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler.core :as rls]
            [rate-limit-scheduler.auth.api-key :as api-key])
  (:import [java.lang Thread]))

(def api-key "abc123")

(defn make []
  (rls/make-system
    {::rls/limit      10000
     ::rls/middleware (partial
                        api-key/middleware
                        {::api-key/log-fn  prn
                         ::api-key/api-key api-key})}))

(defonce system (atom (make)))

(defn post [sleep url reqs]
  (Thread/sleep sleep)
  (http/request
    {:url     url
     :method  :post
     :headers {"api-key" api-key}
     :body    (cheshire/generate-string reqs)}))

(defn requests [n-requests n-in-request]
  (map
    (fn [i]
      (map (fn [ii] (identity (str "david+" i "-" ii "@roikoi.com"))) (range n-in-request)))
    (range n-requests)))

(defn stress-test [sleep url]
  (prn
    (time
      (do
        ;(rls/start @system)
        (let [requests (requests 2048 40)
              posts (doall (map #(post sleep url %) requests))
              resps (map #(cheshire/parse-string (:body @%)) posts)
              [nw nl] (reduce
                        (fn [[aw al] [w l]]
                          (prn [w l])
                          [(+ aw w)
                           (+ al l)])
                        [0 0]
                        (map
                          (fn [[w l]]
                            [(count w)
                             (count l)])
                          resps))]
          ;(rls/stop @system)
          (prn "nw" nw)
          (prn "nl" nl)
          (is (= (mod nw 10) 0)))))))

(defn local-url []
  (str
    "http://localhost:"
    (get-in @@system [::rls/server-options :port])))

(deftest local-stress-test
  (stress-test 10 (local-url)))

(deftest get-ok
  (rls/start @system)
  (is (= (:status @(http/request {:url (local-url) :method :get})) 200))
  (rls/stop @system))

(deftest head-not-allowed
  (rls/start @system)
  (is (= (:status @(http/request {:url (local-url) :method :head})) 405))
  (rls/stop @system))

(comment
  (count (requests 1000 40))
  (cheshire/generate-string [[1] [2] [3]])
  (cheshire/parse-string "[[1], [2], [3]]")

  (reset! system (make))

  (rls/start @system)

  (deref (http/request {:url (local-url) :method :get :timeout 10}))

  (def ret (post 0 (local-url) ["a" "b"]))
  (rls/stop @system)
  (deref ret)
  (deref @system)

  (run-tests)

  (def ret
    (deref (post
             0
             "http://a91e80153c5af11e7814e023c757718d-1067407993.us-west-2.elb.amazonaws.com"
             ["howdy@roikoi.com", "ho@roikoi.com"])
           ))

  (cheshire/parse-string (:body ret))

  (stress-test 100 "http://a91e80153c5af11e7814e023c757718d-1067407993.us-west-2.elb.amazonaws.com")
  )
