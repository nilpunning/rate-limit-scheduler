(ns rate-limit-scheduler.core
  (:require [clojure.java.io :as io]
            [org.httpkit.server :as server]
            [cheshire.core :as cheshire]
            [rate-limit-scheduler
             [split-queue :as sq]])
  (:import [java.lang System Thread Runtime]))

(defn handle-get [channel]
  (server/send! channel {:status 200 :body "Running"}))

(defn handle-post [system req channel]
  (let [reqs (->> req
                  :body
                  io/reader
                  cheshire/parse-stream
                  (map #(identity {::request % ::channel channel})))
        status (dosync
                 (if (::collecting? @system)
                   (if (sq/able? (::collecting-queue @system))
                     (do
                       (alter system update ::collecting-queue sq/put reqs)
                       false)
                     ; Too Many Requests
                     429)
                   ; Service Unavailable
                   503))]
    (when status
      (server/send! channel {:status status}))))

(defn route [system {:keys [request-method] :as req} channel]
  (case request-method
    :get (handle-get channel)
    :post (handle-post system req channel)
    (server/send!
      channel
      {:status 405 :headers {"Allow" "GET, POST"}})))

(defn server-handler [system req]
  (server/with-channel req channel (route system req channel)))

(defn run-server [server-options system]
  (server/run-server
    ((::middleware @system) (partial server-handler system))
    server-options))

(defn reset-collecting-queue [system]
  (dosync
    (let [{:keys [::collecting-queue]} @system
          {:keys [::sq/limit ::sq/last-taken]} collecting-queue]
      (alter
        system
        assoc
        ::collecting-queue
        (sq/make limit last-taken))
      collecting-queue)))

(defn collect [system]
  (let [draining-queue (reset-collecting-queue system)
        {:keys [::poll-size ::request-state ::request-batch ::log-fn]} @system
        [winners loser-queue] (sq/poll draining-queue (poll-size request-state))
        [losers _] (sq/drain loser-queue)
        [new-request-state winner-resps] (request-batch winners)
        winner-groups (group-by ::channel winner-resps)
        loser-groups (group-by ::channel losers)
        channels (set (concat (keys winner-groups) (keys loser-groups)))]
    (dosync (alter system update ::request-state merge new-request-state))
    (doseq [channel channels]
      (server/send!
        channel
        {:status  200
         :headers {"Content-Type" "application/json"}
         :body    (cheshire/generate-string
                    [(map #(dissoc % ::channel) (get winner-groups channel))
                     (map #(dissoc % ::channel) (get loser-groups channel))])}))
    (log-fn
      (merge
        (sq/stats draining-queue)
        (select-keys @system [::request-state])))))

(defn sleep [fn]
  (let [start-time (System/currentTimeMillis)]
    (fn)
    (let [end-time (System/currentTimeMillis)
          diff (- end-time start-time)]
      (when (< diff 2000)
        (Thread/sleep (- 2000 diff))))))

(defn request-loop [system]
  (while (::collecting? @system)
    (sleep
      #(try
         (collect system)
         (catch Exception e
           ((::error-fn @system) e))))))

(defn start-thread [name fn]
  (doto (Thread. ^Runnable fn)
    (.setName name)
    (.start)))

(defn make-system
  "
  Options:
    ::limit             ; Number of simultaneous requestors allowed before
                        ; 429 Too Many Requests returned
    ::server-options    ; Options passed to httpkit server
    ::middleware        ; (fn [handler]) => (fn [req])
    ::poll-size         ; Calculates number of requests to make
                        ; (fn [request-state])
                        ; => int
    ::request-batch     ; Makes the actual request
                        ; (fn [(seq {::request ::channel})])
                        ; => [request-state
                              (seq {::request ::response ::channel})]
    ::log-fn            ; Called ever cycle with stats to log
                        ; (fn [{}]) => nil
    ::error-fn          ; Called when an error occurs
                        ; (fn [exception]) => nil
  "
  [{limit ::limit :as options}]
  (let [limit (or limit 2000)]
    (ref
      (merge
        ; Defaults
        {::server-options {:port 8080 :queue-size limit}
         ::middleware     (fn [handler] (fn [req] (handler req)))
         ::poll-size      (constantly 10)
         ::request-batch  (fn [x] [{} x])
         ::log-fn         prn
         ::error-fn       prn}
        ; Override defaults with options passed in
        (dissoc options ::limit)
        ; Internal state
        {::collecting?      false
         ::running?         false
         ::collecting-queue (sq/make limit)
         ; Typically holds information like:
         ; {x-rate-limit-limit int
         ;  x-rate-limit-reset int
         ;  x-rate-limit-remaining int}
         ::request-state    {}}))))

(defn stop [system]
  (when (::running? @system)
    ((::log-fn @system) "Shutting down.")
    (dosync (alter system assoc ::collecting? false))
    (let [{:keys [::server
                  ::request-loop-thread
                  ::shutdown-hook
                  ::shutting-down?]} @system]
      (.join ^Thread request-loop-thread)
      (server :timeout 60)
      (when (not shutting-down?)
        (.removeShutdownHook (Runtime/getRuntime) shutdown-hook))
      (dosync
        (alter system dissoc ::server ::request-loop-thread ::shutdown-hook)
        (alter system assoc ::running? false))
      ((::log-fn @system) "Done shutting down."))))

(defn shutdown-hook [system]
  (Thread.
    ^Runnable
    (fn []
      ((::log-fn @system) "Received shutdown hook.")
      (dosync (alter system assoc ::shutting-down? true))
      (stop system))))

(defn start [system]
  (when (not (::running? @system))
    (dosync (alter system assoc ::collecting? true ::running? true))
    (let [server (run-server (::server-options @system) system)
          thread (start-thread "request-loop" (partial request-loop system))
          shutdown-hook (shutdown-hook system)]
      (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
      (dosync
        (alter
          system
          assoc
          ::server server
          ::request-loop-thread thread
          ::shutdown-hook shutdown-hook)))))