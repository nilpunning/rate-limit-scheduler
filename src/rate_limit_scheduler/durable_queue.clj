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

(defn complete! [task]
  (dq/complete! task))

(defn stats [durable-queue]
  (get (dq/stats (:queues durable-queue)) (name :queue)))

(defn in-progress? [durable-queue]
  (> (get (stats durable-queue) :in-progress 0) 0))

(defn delete! [durable-queue]
  (let [{:keys [queues]} durable-queue]
    (dq/delete! queues)))
