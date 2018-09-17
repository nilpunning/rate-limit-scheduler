(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [rate-limit-scheduler.core :as rls]
            [rate-limit-scheduler.auth.api-key :as api-key]))

(def api-key "abc123")

(defn make []
  (rls/make-system
    {::rls/limit      10000
     ::rls/middleware (partial
                        api-key/middleware
                        {::api-key/log-fn  prn
                         ::api-key/api-key api-key})}))

(defonce system (atom (make)))

(defn local-url []
  (str
    "http://localhost:"
    (get-in @@system [::rls/server-options :port])))

(deftest get-ok
  (rls/start @system)
  (is (= (:status @(http/request {:url (local-url) :method :get})) 200))
  (rls/stop @system))

(deftest head-not-allowed
  (rls/start @system)
  (is (= (:status @(http/request {:url (local-url) :method :head})) 405))
  (rls/stop @system))

(comment
  (reset! system (make))

  (rls/start @system)

  (rls/stop @system)

  (deref (http/request {:url (local-url) :method :get :timeout 10}))

  (deref @system)

  (run-tests)
  )
