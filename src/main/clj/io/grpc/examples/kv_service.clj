(ns io.grpc.examples.kv-service
  (:refer-clojure :exclude [update])
  (:require [io.grpc.examples.kv-edn :as kv-edn])
  (:import [io.grpc BindableService Status]
           [io.grpc.examples.kv_edn CreateRequest DeleteRequest RetrieveRequest UpdateRequest]
           [io.grpc.stub StreamObserver]
           [java.nio ByteBuffer]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def read-delay-millis 10)
(def write-delay-millis 50)
(def store (atom {}))

(defn simulate-work [millis]
  (try
    (.sleep TimeUnit/MILLISECONDS millis)
    (catch InterruptedException e
      (.. Thread currentThread interrupt)
      (throw (RuntimeException. e)))))

(deftype KeyValueServiceImplBase []
  kv-edn/PKeyValueServiceImplBase

  (create [_ req response-observer]
    (let [key (ByteBuffer/wrap (.key ^CreateRequest req))
          value (ByteBuffer/wrap (.value ^CreateRequest req))]
      (simulate-work write-delay-millis)
      (swap! store assoc key value)
      (if (get @store key)
        (do
          (.onNext ^StreamObserver response-observer (kv-edn/->CreateResponse))
          (.onCompleted ^StreamObserver response-observer)
          nil)
        (.onError ^StreamObserver response-observer (.asRuntimeException Status/ALREADY_EXISTS)))))

  (retrieve [_ req response-observer]
    (let [key (ByteBuffer/wrap (.key ^RetrieveRequest req))]
      (simulate-work read-delay-millis)
      (if-let [value (get @store key)]
        (do
          (.onNext ^StreamObserver response-observer (kv-edn/->RetrieveResponse (.array ^ByteBuffer value)))
          (.onCompleted ^StreamObserver response-observer)
          nil)
        (.onError ^StreamObserver response-observer (.asRuntimeException Status/NOT_FOUND)))))

  (update [_ req response-observer]
    (let [key (ByteBuffer/wrap (.key ^UpdateRequest req))
          value (ByteBuffer/wrap (.value ^UpdateRequest req))]
      (simulate-work write-delay-millis)
      (if (get @store key)
        (do
          (swap! store assoc key value)
          (.onNext ^StreamObserver response-observer (kv-edn/->UpdateResponse))
          (.onCompleted ^StreamObserver response-observer)
          nil)
        (.onError ^StreamObserver response-observer (.asRuntimeException Status/NOT_FOUND)))))

  (delete [_ req response-observer]
    (let [key (ByteBuffer/wrap (.key ^DeleteRequest req))]
      (simulate-work write-delay-millis)
      (swap! store dissoc key)
      (.onNext ^StreamObserver response-observer (kv-edn/->DeleteResponse))
      (.onCompleted ^StreamObserver response-observer)))

  BindableService

  (bindService [this]
    (kv-edn/bind-service this)))

(defn create-kv-service []
  (->KeyValueServiceImplBase))
