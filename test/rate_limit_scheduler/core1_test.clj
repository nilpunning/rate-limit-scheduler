(ns rate-limit-scheduler.core1-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [core1 :as rls]]))

(def server-options {:port 8080})

(defonce system (rls/make-system server-options 3))

(comment
  (rls/stop system)

  (deref
    (http/post
      (str
        "http://localhost:"
        (:port server-options))
      {:body (cheshire/generate-string ["a" "b"])}))
  )