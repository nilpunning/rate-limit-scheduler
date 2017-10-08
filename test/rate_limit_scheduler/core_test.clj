(ns rate-limit-scheduler.core-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [core :as rls]]))

(def server-options {:port 8080})

(defonce system (rls/make-system server-options 3))

(defn post [reqs]
  (deref
    (http/request
      {:url    (str
                 "http://localhost:"
                 (:port server-options))
       :method :post
       :body   (cheshire/generate-string reqs)})))

(comment
  (rls/stop system)
  (reset! system (rls/make-system server-options 3))

  (deref system)
  (post [:a])
  (post [:b :c])
  (post [:d :e])

  )