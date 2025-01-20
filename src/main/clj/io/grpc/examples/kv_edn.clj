(ns io.grpc.examples.kv-edn
  (:refer-clojure :exclude [update])
  (:require [clojure.edn :as edn])
  (:import [io.grpc MethodDescriptor MethodDescriptor$Marshaller MethodDescriptor$MethodType ServerServiceDefinition ServerServiceDefinition$Builder]
           [io.grpc.stub ServerCalls ServerCalls$UnaryMethod StreamObserver]
           [java.io ByteArrayInputStream InputStreamReader PushbackReader]
           [java.nio.charset StandardCharsets]
           [java.util Base64]))

(set! *warn-on-reflection* true)

(declare edn-marshaller)

(def service-name "io.grpc.KeyValueService")

(defrecord CreateRequest [key value])

(defrecord CreateResponse [])

(defn create-method []
  (.. (MethodDescriptor/newBuilder (edn-marshaller) (edn-marshaller))
      (setFullMethodName (MethodDescriptor/generateFullMethodName service-name "Create"))
      (setType MethodDescriptor$MethodType/UNARY)
      (setSampledToLocalTracing true)
      build))

(defrecord RetrieveRequest [key])

(defrecord RetrieveResponse [value])

(defn retrieve-method []
  (.. (MethodDescriptor/newBuilder (edn-marshaller) (edn-marshaller))
      (setFullMethodName (MethodDescriptor/generateFullMethodName service-name "Retrieve"))
      (setType MethodDescriptor$MethodType/UNARY)
      (setSampledToLocalTracing true)
      build))

(defrecord UpdateRequest [key value])

(defrecord UpdateResponse [])

(defn update-method []
  (.. (MethodDescriptor/newBuilder (edn-marshaller) (edn-marshaller))
      (setFullMethodName (MethodDescriptor/generateFullMethodName service-name "Update"))
      (setType MethodDescriptor$MethodType/UNARY)
      (setSampledToLocalTracing true)
      build))

(defrecord DeleteRequest [key])

(defrecord DeleteResponse [])

(defn delete-method []
  (.. (MethodDescriptor/newBuilder (edn-marshaller) (edn-marshaller))
      (setFullMethodName (MethodDescriptor/generateFullMethodName service-name "Delete"))
      (setType MethodDescriptor$MethodType/UNARY)
      (setSampledToLocalTracing true)
      build))

(defprotocol PKeyValueServiceImplBase
  (create [this req response-observer])
  (retrieve [this req response-observer])
  (update [this req response-observer])
  (delete [this req response-observer]))

(defn bind-service [this]
    (let [ssd (ServerServiceDefinition/builder ^String service-name)]
      (.addMethod ^ServerServiceDefinition$Builder ssd
                  (create-method)
                  (ServerCalls/asyncUnaryCall 
                    (reify ServerCalls$UnaryMethod
                      (invoke [_ request response-observer]
                        (create this ^CreateRequest request ^StreamObserver response-observer)))))
      (.addMethod ^ServerServiceDefinition$Builder ssd
                  (retrieve-method)
                  (ServerCalls/asyncUnaryCall 
                    (reify ServerCalls$UnaryMethod
                      (invoke [_ request response-observer]
                        (retrieve this ^RetrieveRequest request ^StreamObserver response-observer)))))
      (.addMethod ^ServerServiceDefinition$Builder ssd
                  (update-method)
                  (ServerCalls/asyncUnaryCall 
                    (reify ServerCalls$UnaryMethod
                      (invoke [_ request response-observer]
                        (update this ^UpdateRequest request ^StreamObserver response-observer)))))
      (.addMethod ^ServerServiceDefinition$Builder ssd
                  (delete-method)
                  (ServerCalls/asyncUnaryCall 
                    (reify ServerCalls$UnaryMethod
                      (invoke [_ request response-observer]
                        (delete this ^DeleteRequest request ^StreamObserver response-observer)))))
      (.build ssd)))



(defmethod print-method (Class/forName "[B") [bytes ^java.io.Writer writer]
  (doto writer
    (.write "#bytes \"")
    (.write (.encodeToString (Base64/getEncoder) bytes))
    (.write "\"")))

(defn edn-marshaller []
  (reify MethodDescriptor$Marshaller
    (stream [_ value]
      (-> (prn-str value)
          (.getBytes  StandardCharsets/UTF_8)
          (ByteArrayInputStream.)))
    (parse [_ stream]
      (edn/read {:readers {'bytes #(.decode (Base64/getDecoder) ^String %)
                           `CreateRequest map->CreateRequest
                           `CreateResponse map->CreateResponse
                           `RetrieveRequest map->RetrieveRequest
                           `RetrieveResponse map->RetrieveResponse
                           `UpdateRequest map->UpdateRequest
                           `UpdateResponse map->UpdateResponse
                           `DeleteRequest map->DeleteRequest
                           `DeleteResponse map->DeleteResponse}}
                (-> stream
                    (InputStreamReader. StandardCharsets/UTF_8)
                    (PushbackReader.))))))
