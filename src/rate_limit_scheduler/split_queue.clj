(ns rate-limit-scheduler.split-queue
  (:import [java.lang Thread]
           [clojure.lang PersistentQueue]))

(defn round-robin [s last-taken]
  (first (apply concat (reverse (split-with (partial >= last-taken) s)))))

(defn next-queue [split-queue]
  (let [{:keys [::last-taken ::queues]} split-queue
        i (round-robin (range (count queues)) last-taken)]
    [(get queues i) i]))

(defn vec-remove [coll pos]
  (vec (concat (subvec coll 0 pos) (subvec coll (inc pos)))))

(defn queue-pop [queues i]
  (let [new-queue (vec (rest (get queues i)))]
    (if (empty? new-queue)
      (vec-remove queues i)
      (assoc queues i new-queue))))

(defn make
  "
  limit            ; max number of queues
  init-round-robin ; initial value used in round-robin comparision
  "
  ([limit init-round-robin]
   {::limit      limit
    ::n          0
    ::queues     []
    ::last-taken init-round-robin})
  ([limit]
   (make limit -1)))

(defn count-sq [split-queue]
  (let [{queues ::queues} split-queue]
    (reduce (fn [a queue] (+ a (count queue))) 0 queues)))

(defn stats [split-queue]
  (let [{queues ::queues} split-queue]
    {:n        (count-sq split-queue)
     :n-queues (count queues)}))

(defn able? [split-queue]
  (let [{:keys [::limit ::queues]} split-queue]
    (< (count queues) limit)))

(defn put [split-queue vals]
  (if (able? split-queue)
    (update split-queue ::queues conj vals)
    split-queue))

(defn poll
  ([split-queue]
   (let [[queue i] (next-queue split-queue)]
     [(first queue)
      (if i
        (-> split-queue
            (update ::queues queue-pop i)
            (assoc ::last-taken i))
        split-queue)]))
  ([split-queue n]
   (loop [vals []
          split-queue split-queue]
     (if (and (< (count vals) n)
              (> (count-sq split-queue) 0))
       (let [[val new-sq] (poll split-queue)]
         (recur (conj vals val) new-sq))
       [vals split-queue]))))

(defn drain [split-queue]
  (poll split-queue (count-sq split-queue)))