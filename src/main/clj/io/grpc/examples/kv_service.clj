(ns io.grpc.examples.kv-service
  (:import [com.google.protobuf ByteString]
           [io.grpc Status]
           [io.grpc.examples.proto CreateRequest CreateResponse DeleteRequest DeleteResponse KeyValueServiceGrpc$KeyValueServiceImplBase RetrieveRequest RetrieveResponse UpdateRequest UpdateResponse]
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

(defn create-kv-service []
  (proxy [KeyValueServiceGrpc$KeyValueServiceImplBase] []

    (create [^CreateRequest req ^StreamObserver response-observer]
      (let [key (.. req getKey asReadOnlyByteBuffer)
            value (.. req getValue asReadOnlyByteBuffer)]
        (simulate-work write-delay-millis)
        (swap! store assoc key value)
        (if (get @store key)
          (do
            (.onNext response-observer (CreateResponse/getDefaultInstance))
            (.onCompleted response-observer)
            nil)
          (.onError response-observer (.asRuntimeException Status/ALREADY_EXISTS)))))

    (retrieve [^RetrieveRequest req ^StreamObserver response-observer]
      (let [key (.. req getKey asReadOnlyByteBuffer)]
        (simulate-work read-delay-millis)
        (if-let [value (get @store key)]
          (do
            (.onNext response-observer
                     (.. (RetrieveResponse/newBuilder)
                         (setValue (ByteString/copyFrom ^ByteBuffer (.slice ^ByteBuffer value)))
                         build))
            (.onCompleted response-observer)
            nil)
          (.onError response-observer (.asRuntimeException Status/NOT_FOUND)))))

    (update [^UpdateRequest req ^StreamObserver response-observer]
      (let [key (.. req getKey asReadOnlyByteBuffer)
            value (.. req getValue asReadOnlyByteBuffer)]
        (simulate-work write-delay-millis)
        (if (get @store key)
          (do
            (swap! store assoc key value)
            (.onNext response-observer (UpdateResponse/getDefaultInstance))
            (.onCompleted response-observer)
            nil)
          (.onError response-observer (.asRuntimeException Status/NOT_FOUND)))))

    (delete [^DeleteRequest req ^StreamObserver response-observer]
      (let [key (.. req getKey asReadOnlyByteBuffer)]
        (simulate-work write-delay-millis)
        (swap! store dissoc key)
        (.onNext response-observer (DeleteResponse/getDefaultInstance))
        (.onCompleted response-observer)))))
