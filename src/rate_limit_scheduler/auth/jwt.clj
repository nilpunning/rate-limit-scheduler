(ns rate-limit-scheduler.auth.jwt
  (:require [clojure.java.io :as io]
            [byte-streams :as byte-streams])
  (:import [java.io InputStream UnsupportedEncodingException]
           [com.auth0.jwt JWT]
           [com.auth0.jwt.algorithms Algorithm]
           [com.auth0.jwt.exceptions JWTVerificationException]))

(defn algorithm [secret]
  (Algorithm/HMAC256 ^String secret))

(defn encode [secret body]
  (-> (JWT/create)
      (.withClaim "body" ^String body)
      (.sign (algorithm secret))))

(defn decode [token secret log-fn]
  (try
    (-> (algorithm secret)
        JWT/require
        .build
        (.verify (byte-streams/convert token String))
        (.getClaim "body")
        .asString
        (byte-streams/convert InputStream))
    (catch UnsupportedEncodingException ex
      (log-fn ex))
    (catch JWTVerificationException ex
      (log-fn ex))))

(defn middleware [log-fn secret handler]
  (fn [req]
    (case (:request-method req)
      :post (handler (update req :body decode secret log-fn))
      (handler req))))