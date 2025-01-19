(ns io.grpc.examples.kv-client
  (:require [clojure.tools.logging.readable :as log])
  (:import [com.google.protobuf ByteString]
           [com.google.common.util.concurrent FutureCallback Futures MoreExecutors]
           [io.grpc Status Status$Code]
           [io.grpc.examples.proto CreateRequest CreateResponse DeleteRequest DeleteResponse KeyValueServiceGrpc KeyValueServiceGrpc$KeyValueServiceFutureStub RetrieveRequest RetrieveResponse UpdateRequest UpdateResponse]
           [java.util Random]
           [java.util.concurrent Semaphore]))

(set! *warn-on-reflection* true)

(def mean-value-size (int 65536))
(def known-keys (atom #{}))
(def rpc-count (atom 0))
(def limiter (Semaphore. 100))

(defn create-random-bytes [mean]
  (let [random (Random.)
        size (int (Math/round (* mean (- 0 (Math/log (- 1 (.nextDouble random)))))))
        bytes (byte-array (+ 1 size))]
    (.nextBytes random bytes)
    (ByteString/copyFrom bytes)))

(defn create-random-key []
  (locking known-keys
    (loop [key (create-random-bytes mean-value-size)
           size (count @known-keys)]
      (if (> (count (swap! known-keys conj key)) size)
        key
        (recur (create-random-bytes mean-value-size)
               (inc size))))))

(defn do-create [^KeyValueServiceGrpc$KeyValueServiceFutureStub stub error]
  (.acquire ^Semaphore limiter)
  (let [key (create-random-key)
        res (.create stub
                     (.. (CreateRequest/newBuilder)
                         (setKey key)
                         (setValue (create-random-bytes mean-value-size))
                         build))]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ result]
          (when-not (.equals result (CreateResponse/getDefaultInstance))
            (reset! error (RuntimeException. "Invalid response (create)")))
          (locking known-keys
            (swap! known-keys conj key)))
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/ALREADY_EXISTS)
              (locking known-keys
                (swap! known-keys disj key)
                  (log/info "Key already existed"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-retrieve [^KeyValueServiceGrpc$KeyValueServiceFutureStub stub error]
  (.acquire ^Semaphore limiter)
  (let [key (rand-nth (seq @known-keys))
        res (.retrieve stub
                       (.. (RetrieveRequest/newBuilder)
                           (setKey key)
                           build))]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ result]
          (when (< (.. ^RetrieveResponse result getValue size) 1)
            (reset! error (RuntimeException. "Invalid response (retrieve)"))))
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/NOT_FOUND)
              (locking known-keys
                (swap! known-keys disj key)
                  (log/info "Key not found (retrieve)"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-update [^KeyValueServiceGrpc$KeyValueServiceFutureStub stub error]
  (.acquire ^Semaphore limiter)
  (let [key (rand-nth (seq @known-keys))
        res (.update stub
                     (.. (UpdateRequest/newBuilder)
                         (setKey key)
                         (setValue (create-random-bytes mean-value-size))
                         build))]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ result]
          (when-not (.equals result (UpdateResponse/getDefaultInstance))
            (reset! error (RuntimeException. "Invalid response (update)"))))
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/NOT_FOUND)
              (locking known-keys
                (swap! known-keys disj key)
                  (log/info "Key not found (udpate)"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-delete [^KeyValueServiceGrpc$KeyValueServiceFutureStub stub error]
  (.acquire ^Semaphore limiter)
  (let [key (rand-nth (seq @known-keys))]
    (locking known-keys
      (swap! known-keys disj key))
    (let [res (.delete stub
                       (.. (DeleteRequest/newBuilder)
                           (setKey key)
                           build))]
      (.addListener res
                    #(do (swap! rpc-count inc)
                         (.release ^Semaphore limiter))
                    (MoreExecutors/directExecutor))
      (Futures/addCallback res
        (reify FutureCallback
          (onSuccess [_ result]
            (when-not (.equals result (DeleteResponse/getDefaultInstance))
              (throw (RuntimeException. "Invalid response"))))
          (onFailure [_ throwable]
            (let [status (Status/fromThrowable throwable)]
              (if (= (.getCode status) Status$Code/NOT_FOUND)
                (log/info "Key not found (delete)")
                (reset! error throwable)))))
        (MoreExecutors/directExecutor)))))

(defn do-client-work [channel done]
  (let [random (Random.)
        stub (KeyValueServiceGrpc/newFutureStub channel)
        errors (atom nil)]
    (while (and (not @done) (nil? @errors))
      (let [command (.nextInt random 4)]
        (if (= command 0)
          (do-create stub errors)
          (when (seq @known-keys)
            (case command
              1 (do-retrieve stub errors)
              2 (do-update stub errors)
              3 (do-delete stub errors)
              (AssertionError.))
            (when (some? @errors)
              (throw (RuntimeException. ^Throwable @errors)))))))))
