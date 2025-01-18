(ns io.grpc.examples.kv-runner
  (:require [io.grpc.examples.kv-client :refer [do-client-work rpc-count]]
            [io.grpc.examples.kv-service :refer [create-kv-service]]
            [clojure.tools.logging.readable :as log])
  (:import [io.grpc ManagedChannelBuilder ServerBuilder]
           [java.util.concurrent Executors TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean])
  (:gen-class))


(def duration-seconds 60)

(defn run-client [server]
  (let [channel (.. ManagedChannelBuilder
                    (forTarget (str "dns:///localhost:" (.getPort server)))
                    usePlaintext
                    build)
        scheduler (Executors/newSingleThreadScheduledExecutor)]
    (try
      (let [done (AtomicBoolean.)]
        (log/info "Starting client work")
        (.schedule scheduler #(.set done true) duration-seconds TimeUnit/SECONDS)
        (do-client-work channel done)
        (log/info (str "Did %f RPCs/s" (double (/ @rpc-count duration-seconds)))))
      (finally
        (log/info "Completed client work")
        (.shutdownNow scheduler)
        (.shutdownNow channel)))))

(defn start-server []
  (let [server (.. ServerBuilder
                   (forPort 0)
                   (addService (create-kv-service))
                   build)]
    (log/info "Starting server")
    (.start server)))

(defn stop-server [server]
  (log/info "Stopping server")
  (.shutdown server)
  (when-not (.awaitTermination server 1 TimeUnit/SECONDS)
    (.shutdownNow server)
    (when-not (.awaitTermination server 1 TimeUnit/SECONDS)
      (throw (RuntimeException. "Unable to shutdown server")))))

(defn -main [& _]
  (let [server (start-server)]
    (try
      (run-client server)
      (finally
        (stop-server server)))))
