(ns io.grpc.examples.kv-client
  (:require [clojure.tools.logging.readable :as log])
  (:import [com.google.protobuf ByteString]
           [io.grpc Status$Code StatusRuntimeException]
           [io.grpc.examples.proto CreateRequest CreateResponse DeleteRequest DeleteResponse KeyValueServiceGrpc KeyValueServiceGrpc$KeyValueServiceBlockingStub RetrieveRequest RetrieveResponse UpdateRequest UpdateResponse]
           [java.util Random]
           [java.util.concurrent.atomic AtomicBoolean]))

(def known-keys (atom #{}))
(def mean-value-size (int 65536))
(def rpc-count (atom 0))

(defn create-random-bytes [mean]
  (let [random (Random.)
        size (int (Math/round (* mean (- 0 (Math/log (- 1 (.nextDouble random)))))))
        bytes (byte-array (+ 1 size))]
    (.nextBytes random bytes)
    (ByteString/copyFrom bytes)))

(defn create-random-key []
  (loop [key (create-random-bytes mean-value-size)
         size (count @known-keys)]
    (if (> (count (swap! known-keys conj key)) size)
      key
      (recur (create-random-bytes mean-value-size)
             (inc size)))))

(defn do-create [^KeyValueServiceGrpc$KeyValueServiceBlockingStub stub]
  (let [key (create-random-key)]
    (try
      (let [res (.create stub
                         (.. (CreateRequest/newBuilder)
                             (setKey key)
                             (setValue (create-random-bytes mean-value-size))
                             build))]
        (when-not (.equals res (CreateResponse/getDefaultInstance))
          (throw (RuntimeException. "Invalid response"))))
      (catch StatusRuntimeException e
        (if (= (.. e getStatus getCode) Status$Code/ALREADY_EXISTS)
          (do
            (swap! known-keys disj key)
            (log/info e "Key already existed"))
          (throw e))))))

(defn do-retrieve [^KeyValueServiceGrpc$KeyValueServiceBlockingStub stub]
  (let [key (rand-nth (seq @known-keys))]
    (try
      (let [^RetrieveResponse res (.retrieve stub
                                             (.. (RetrieveRequest/newBuilder)
                                                 (setKey key)
                                                 build))]
        (when (< (.. res getValue size) 1)
          (throw (RuntimeException. "Invalid response"))))
      (catch StatusRuntimeException e
        (if (= (.. e getStatus getCode) Status$Code/NOT_FOUND)
          (do
            (swap! known-keys disj key)
            (log/info e "Key not found"))
          (throw e))))))

(defn do-update [^KeyValueServiceGrpc$KeyValueServiceBlockingStub stub]
  (let [key (rand-nth (seq @known-keys))]
    (try
      (let [res (.update stub
                         (.. (UpdateRequest/newBuilder)
                             (setKey key)
                             (setValue (create-random-bytes mean-value-size))
                             build))]
        (when-not (.equals res (UpdateResponse/getDefaultInstance))
          (throw (RuntimeException. "Invalid response"))))
      (catch StatusRuntimeException e
        (if (= (.. e getStatus getCode) Status$Code/NOT_FOUND)
          (do
            (swap! known-keys disj key)
            (log/info e "Key not found"))
          (throw e))))))

(defn do-delete [^KeyValueServiceGrpc$KeyValueServiceBlockingStub stub]
  (let [key (rand-nth (seq @known-keys))
        res (.delete stub
                     (.. (DeleteRequest/newBuilder)
                         (setKey key)
                         build))]
    (swap! known-keys disj key)
    (when-not (.equals res (DeleteResponse/getDefaultInstance))
      (throw (RuntimeException. "Invalid response")))))

(defn do-client-work [channel ^AtomicBoolean done]
  (let [random (Random.)
        stub (KeyValueServiceGrpc/newBlockingStub channel)]
    (while (not (.get done))
      (let [command (.nextInt random 4)]
        (if (= command 0)
          (do-create stub)
          (when (seq @known-keys)
            (case command
              1 (do-retrieve stub)
              2 (do-update stub)
              3 (do-delete stub)
              (AssertionError.))
            (swap! rpc-count inc)))))))
