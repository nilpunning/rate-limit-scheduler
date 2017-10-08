(ns rate-limit-scheduler.core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [org.httpkit.server :as server]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [split-queue :as sq]]))

(defn put [system channel reqs]
  (update
    system
    ::filling-queue
    (fn [fq]
      (->> reqs
           (map #(identity {::request % ::channel channel}))
           (sq/put fq)))))

(defn do-handle [req system channel]
  (server/on-close
    channel
    (fn [status]
      (println "channel closed" status)))

  (let [reqs (cheshire/parse-stream (io/reader (:body req)))]
    (swap! system put channel reqs))

  (server/send!
    channel
    {:status  200
     :headers {"Content-Type" "text/html"}
     :body    "hello ppls"}
    false)

  (server/close channel))

(defn server-handler [system req]
  (server/with-channel req channel
    (do-handle req system channel)))

(defn run-server [server-options system]
  (server/run-server
    (partial #'server-handler system)
    server-options))

(defn make-system [server-options limit]
  (let [system (atom {})]
    (reset!
      system
      {::server        (run-server server-options system)
       ::filling-queue (sq/make limit 0)
       ::draining-loop (sq/make limit 0)})
    system))

(defn stop [system]
  ((::server @system) :timeout 0))
