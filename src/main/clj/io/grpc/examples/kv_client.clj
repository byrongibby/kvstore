(ns io.grpc.examples.kv-client
  (:require [clojure.tools.logging.readable :as log]
            [io.grpc.examples.kv-edn :as kv-edn])
  (:import [com.google.common.util.concurrent FutureCallback Futures MoreExecutors]
           [io.grpc CallOptions Channel Status Status$Code]
           [io.grpc.examples.kv_edn RetrieveResponse]
           [io.grpc.stub ClientCalls]
           [java.nio ByteBuffer]
           [java.util Random HashSet]
           [java.util.concurrent Semaphore]))

(set! *warn-on-reflection* true)

(def mean-value-size (int 65536))
(def known-keys (HashSet.))
(def rpc-count (atom 0))
(def limiter (Semaphore. 100))

(defn create-random-bytes [mean]
  (let [random (Random.)
        size (int (Math/round (* mean (- 0 (Math/log (- 1 (.nextDouble random)))))))
        bytes (byte-array (+ 1 size))]
    (.nextBytes random bytes)
    (ByteBuffer/wrap bytes)))

(defn create-random-key []
  (loop [key (create-random-bytes mean-value-size)]
    (if-not (.contains ^HashSet known-keys key)
      key
      (recur (create-random-bytes mean-value-size)))))

(defn do-create [^Channel chan error]
  (.acquire ^Semaphore limiter)
  (let [key (create-random-key)
        call (.newCall chan (kv-edn/create-method) CallOptions/DEFAULT)
        req (kv-edn/->CreateRequest (.array key) (.array (create-random-bytes mean-value-size)))
        res (ClientCalls/futureUnaryCall call req)]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ _]
          (locking known-keys
            (.add ^HashSet known-keys key)))
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/ALREADY_EXISTS)
              (locking known-keys
                (.remove ^HashSet known-keys key)
                (log/info "Key already existed"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-retrieve [^Channel chan error]
  (.acquire ^Semaphore limiter)
  (let [key (locking known-keys (rand-nth (seq known-keys)))
        call (.newCall chan (kv-edn/retrieve-method) CallOptions/DEFAULT)
        req (kv-edn/->RetrieveRequest (.array key))
        res (ClientCalls/futureUnaryCall call req)]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ result]
          (when (< (count (.value ^RetrieveResponse result)) 1)
            (reset! error (RuntimeException. "Invalid response (retrieve)"))))
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/NOT_FOUND)
              (locking known-keys
                (.remove ^HashSet known-keys key)
                (log/info "Key not found (retrieve)"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-update [^Channel chan error]
  (.acquire ^Semaphore limiter)
  (let [key (locking known-keys (rand-nth (seq known-keys)))
        call (.newCall chan (kv-edn/update-method) CallOptions/DEFAULT)
        req (kv-edn/->UpdateRequest (.array key) (.array (create-random-bytes mean-value-size)))
        res (ClientCalls/futureUnaryCall call req)]
    (.addListener res
                  #(do (swap! rpc-count inc)
                       (.release ^Semaphore limiter))
                  (MoreExecutors/directExecutor))
    (Futures/addCallback res
      (reify FutureCallback
        (onSuccess [_ _]
          )
        (onFailure [_ throwable]
          (let [status (Status/fromThrowable throwable)]
            (if (= (.getCode status) Status$Code/NOT_FOUND)
              (locking known-keys
                (.remove ^HashSet known-keys key)
                (log/info "Key not found (udpate)"))
              (reset! error throwable)))))
      (MoreExecutors/directExecutor))))

(defn do-delete [^Channel chan error]
  (.acquire ^Semaphore limiter)
  (let [key (locking known-keys (rand-nth (seq known-keys)))]
    (locking known-keys
      (.remove ^HashSet known-keys key))
    (let [call (.newCall chan (kv-edn/delete-method) CallOptions/DEFAULT)
          req (kv-edn/->DeleteRequest (.array key))
          res (ClientCalls/futureUnaryCall call req)]
      (.addListener res
                    #(do (swap! rpc-count inc)
                         (.release ^Semaphore limiter))
                    (MoreExecutors/directExecutor))
      (Futures/addCallback res
        (reify FutureCallback
          (onSuccess [_ _]
            )
          (onFailure [_ throwable]
            (let [status (Status/fromThrowable throwable)]
              (if (= (.getCode status) Status$Code/NOT_FOUND)
                (log/info "Key not found (delete)")
                (reset! error throwable)))))
        (MoreExecutors/directExecutor)))))

(defn do-client-work [channel done]
  (let [random (Random.)
        errors (atom nil)]
    (while (and (not @done) (nil? @errors))
      (let [command (.nextInt random 4)]
        (if (= command 0)
          (do-create channel errors)
          (when (seq known-keys)
            (case command
              1 (do-retrieve channel errors)
              2 (do-update channel errors)
              3 (do-delete channel errors)
              (AssertionError.))
            (when (some? @errors)
              (throw (RuntimeException. ^Throwable @errors)))))))))
