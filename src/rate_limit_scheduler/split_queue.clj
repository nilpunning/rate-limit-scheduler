(ns rate-limit-scheduler.split-queue
  (:import [java.lang Thread]
           [clojure.lang PersistentQueue]))

(defn queue-conj [queue v]
  (conj (if (nil? queue) PersistentQueue/EMPTY queue) v))

(defn queue-put [split-queue k v]
  (-> split-queue
      (update-in [::map-queues k] queue-conj v)
      (update ::n inc)))

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

(defn queue-take [split-queue k]
  (-> split-queue
      (update ::map-queues queue-pop k)
      (update ::n dec)))

(defn make [limit init-round-robin]
  "limit            - max number of items in datastructure
   init-round-robin - initial value used in round-robin comparision"
  (ref {::limit      limit
        ::n          0
        ::map-queues (sorted-map)
        ::last-taken init-round-robin}))

(defn put! [split-queue k v]
  "Returns true if under the limit and able to put"
  (dosync
    (let [{:keys [::limit ::n]} @split-queue
          able? (<= n limit)]
      (when able?
        (alter split-queue queue-put k v))
      able?)))

(defn take! [split-queue]
  "Returns [able? val]
   able? - true if there was something to take
   val   - value taken"
  (dosync
    (let [[queue k] (next-queue @split-queue)
          able? (not (nil? k))]
      (when able?
        (alter split-queue queue-take k))
      [able? (first queue)])))

(comment
  (def sq (make 1 Long/MIN_VALUE))

  (take! sq)

  (put! sq 0 {:hello :world})

  )