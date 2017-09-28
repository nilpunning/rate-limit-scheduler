(ns rate-limit-scheduler.split-queue
  (:import [java.lang Thread]
           [clojure.lang PersistentQueue]))

(defn queue-conj [queue v]
  (conj (if (nil? queue) PersistentQueue/EMPTY queue) v))

; If split-with proves to be a performance bottleneck,
; replace the sorted-map with a data.avl version:
; https://github.com/clojure/data.avl
(defn round-robin [s last-taken]
  (first (apply concat (reverse (split-with (partial >= last-taken) s)))))

(defn next-queue [split-queue]
  (let [{:keys [::last-taken ::map-queues]} split-queue
        k (round-robin (keys map-queues) last-taken)]
    [(get map-queues k) k]))

(defn queue-pop [map-queues k]
  (let [new-queue (pop (get map-queues k))]
    (if (empty? new-queue)
      (dissoc map-queues k)
      (assoc map-queues k new-queue))))

(defn make [limit init-round-robin]
  "limit            - max number of items in datastructure
   init-round-robin - initial value used in round-robin comparision"
  {::limit      limit
   ::n          0
   ::map-queues (sorted-map)
   ::last-taken init-round-robin})

(defn stats [split-queue]
  {:n            (::n split-queue)
   :n-map-queues (count (::map-queues split-queue))})

(defn put [split-queue k v]
  (let [{:keys [::limit ::n]} split-queue
        able? (< n limit)]
    [able?
     (if able?
       (-> split-queue
           (update-in [::map-queues k] queue-conj v)
           (update ::n inc))
       split-queue)]))

(defn poll
  ([split-queue]
   (let [[queue k] (next-queue split-queue)
         able? (boolean k)]
     [able?
      (first queue)
      (if able?
        (-> split-queue
            (update ::map-queues queue-pop k)
            (update ::n dec)
            (assoc ::last-taken k))
        split-queue)]))
  ([split-queue n]
   (loop [vals []
          split-queue split-queue]
     (if (< (count vals) n)
       (let [[able? val new-sq] (poll split-queue)]
         (if able?
           (recur (conj vals val) new-sq)
           [vals split-queue]))
       [vals split-queue]))))