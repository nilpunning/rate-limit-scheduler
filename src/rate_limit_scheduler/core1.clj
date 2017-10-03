(ns rate-limit-scheduler.core1
  (:require [clojure
             [java.io :as io]
             [core.async :as a]]
            [org.httpkit.server :as server]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [split-queue :as sq]]))

(defn respond [])

(defn put [system channel reqs]
  (dosync
    (let [{:keys [filling-queue i]} system
          i (inc i)]
      (alter system assoc ::i i)
      (loop [remaining-reqs reqs]
        (let [req {::request (first remaining-reqs)
                   ::channel channel}
              [able? new-sq] (sq/put filling-queue i req)]
          (if able?
            (do
              (alter system assoc ::filling-queue new-sq)
              (recur (rest remaining-reqs)))
            (rest remaining-reqs))))
      (reduce
        (fn [ b])
        reqs)

      )))

(defn do-handle [req system channel]
  (println req)

  (server/on-close
    channel
    (fn [status]
      (println "channel closed" status)))

  (let [reqs (cheshire/parse-stream (io/reader (:body req)))]
    (put system channel reqs))

  (server/send!
    channel
    {:status  200
     :headers {"Content-Type" "text/html"}
     :body    "hello ppls"})

  (server/close channel))

(defn server-handler [req system put]
  (server/with-channel req channel
    (do-handle req system channel)))

(defn run-server [server-options system]
  (server/run-server
    (partial #'server-handler system)
    server-options))

(defn make-system [server-options limit]
  (let [system (ref {})]
    (dosync
      (ref-set
        system
        {::server        (run-server server-options system)
         ::i             0
         ::filling-queue (sq/make limit 0)
         ::draining-loop (sq/make limit 0)}))
    system))

(defn stop [system]
  ((::server @system) :timeout 0))
