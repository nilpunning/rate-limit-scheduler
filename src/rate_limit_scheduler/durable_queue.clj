(ns rate-limit-scheduler.durable-queue
  (:require [durable-queue :as dq]))

(defn make! [timeout]
  {:queues  (dq/queues "./data" {})
   :timeout timeout})

(defn put! [durable-queue val]
  (let [{:keys [queues timeout]} durable-queue]
    (dq/put! queues :queue val timeout)))

(defn take! [durable-queue]
  (let [{:keys [queues timeout]} durable-queue]
    (dq/take! queues :queue timeout :timeout)))

(defn delete! [durable-queue]
  (let [{:keys [queues]} durable-queue]
    (dq/delete! queues)))