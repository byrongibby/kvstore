(ns io.grpc.examples.kv-runner
  (:require [io.grpc.examples.kv-client :refer [do-client-work rpc-count]]
            [io.grpc.examples.kv-service :refer [create-kv-service]]
            [clojure.tools.logging.readable :as log])
  (:import [io.grpc ManagedChannelBuilder Server ServerBuilder]
           [io.grpc.examples.proto KeyValueServiceGrpc$KeyValueServiceImplBase]
           [java.util.concurrent Executors TimeUnit])
  (:gen-class))

(set! *warn-on-reflection* true)

(def duration-seconds 60)

(defn run-client [^Server server]
  (let [channel (.. ManagedChannelBuilder
                    (forTarget (str "dns:///localhost:" (.getPort server)))
                    usePlaintext
                    build)
        scheduler (Executors/newSingleThreadScheduledExecutor)]
    (try
      (let [done (atom false)]
        (log/info "Starting client work")
        (.schedule scheduler
                   ^java.lang.Runnable #(reset! done true)
                   ^long duration-seconds
                   TimeUnit/SECONDS)
        (do-client-work channel done)
        (log/info (format "Did %f RPCs/s" (double (/ @rpc-count duration-seconds)))))
      (finally
        (log/info "Completed client work")
        (.shutdownNow scheduler)
        (.shutdownNow channel)))))

(defn start-server []
  (let [server (.. ServerBuilder
                   (forPort 0)
                   (addService ^KeyValueServiceGrpc$KeyValueServiceImplBase (create-kv-service))
                   build)]
    (log/info "Starting server")
    (.start ^Server server)))

(defn stop-server [server]
  (log/info "Stopping server")
  (.shutdown ^Server server)
  (when-not (.awaitTermination ^Server server 1 TimeUnit/SECONDS)
    (.shutdownNow ^Server server)
    (when-not (.awaitTermination ^Server server 1 TimeUnit/SECONDS)
      (throw (RuntimeException. "Unable to shutdown server")))))

(defn -main [& _]
  (let [server (start-server)]
    (try
      (run-client server)
      (finally
        (stop-server server)))))
