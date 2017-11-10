(defproject rate-limit-scheduler "0.2.0"
  :description "Schedules requests to a rate limited service"
  :url "https://github.com/nilpunning/rate-limit-scheduler"
  :license {:name "Apache License, Version 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [http-kit "2.2.0"]
                 [cheshire "5.8.0"]
                 [com.auth0/java-jwt "3.3.0"]
                 [byte-streams "0.2.3"]])
