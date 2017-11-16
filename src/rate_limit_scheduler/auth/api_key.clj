(ns rate-limit-scheduler.auth.api-key)

(defn post [{:keys [::log-fn ::api-key]} {:keys [headers] :as req} handler]
  (if (= api-key (get headers "api-key"))
    (handler req)
    {:status 401}))

(defn middleware [options handler]
  (fn [req]
    (case (:request-method req)
      :post (post options req handler)
      (handler req))))