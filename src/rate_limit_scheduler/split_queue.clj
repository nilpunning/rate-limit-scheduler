(ns rate-limit-scheduler.split-queue
  (:import [java.lang Thread]
           [clojure.lang PersistentQueue]))

(defn queue-conj [queue v]
  (conj (if (nil? queue) PersistentQueue/EMPTY queue) v))

(defn queue-put [k v split-queue]
  (let [{:keys [::limit ::n]} split-queue]
    (if (<= n limit)
      (-> split-queue
          (update-in ::map-queues k queue-conj v)
          (update ::n inc))
      split-queue)))

; If split-with proves to be a performance bottleneck,
; replace the sorted-map with a data.avl version:
; https://github.com/clojure/data.avl

(defn round-robin [s last-taken]
  (first (apply concat (reverse (split-with (partial >= last-taken) s)))))

(defn next-queue [split-queue]
  (let [{:keys [::last-taken ::map-queues]} split-queue
        k (round-robin (keys map-queues) last-taken)]
    [k (get map-queues k)]))

(defn queue-pop [split-queue k queue]
  (let [new-queue (pop queue)]
    (if (empty? new-queue)
      (dissoc split-queue k)
      (assoc split-queue k new-queue))))

(defn spin-update! [split-queue fn]
  (loop []
    (let [new-split-queue (swap! split-queue fn)]
      (if (= split-queue new-split-queue)
        (do
          (Thread/sleep 1000)
          (recur))
        new-split-queue))))

(defn make [limit]
  (atom {::limit      limit
         ::n          0
         ::map-queues (sorted-map)
         ::last-taken Long/MIN_VALUE}))

(defn put! [split-queue k v]
  (spin-update! split-queue (partial queue-put k v)))

(defn take! [split-queue k]
  (let [[k queue] (next-queue @split-queue)]
    (spin-update! split-queue (partial queue-pop k queue))
    (swap! update split-queue queue-pop k queue)
    [split-queue (first queue)]))

(comment
  (-> PersistentQueue/EMPTY
      (conj 1 2 3)
      (conj 4)

      first)
  )