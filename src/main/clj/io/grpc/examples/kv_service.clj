(ns io.grpc.examples.kv-service
  (:import [com.google.protobuf ByteString]
           [io.grpc Status]
           [io.grpc.examples.proto CreateRequest CreateResponse DeleteRequest DeleteResponse KeyValueServiceGrpc$KeyValueServiceImplBase RetrieveRequest RetrieveResponse UpdateRequest UpdateResponse]
           [io.grpc.stub StreamObserver]
           [java.nio ByteBuffer]
           [java.util HashMap]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def read-delay-millis 10)
(def write-delay-millis 50)
(def store (HashMap.))

(defn simulate-work [millis]
  (try
    (.sleep TimeUnit/MILLISECONDS millis)
    (catch InterruptedException e
      (.. Thread currentThread interrupt)
      (throw (RuntimeException. e)))))

(defn create-kv-service []
  (proxy [KeyValueServiceGrpc$KeyValueServiceImplBase] []

    (create [^CreateRequest req ^StreamObserver response-observer]
      (locking store
        (let [key (.. req getKey asReadOnlyByteBuffer)
              value (.. req getValue asReadOnlyByteBuffer)]
          (simulate-work write-delay-millis)
          (if (nil? (.putIfAbsent ^HashMap store key value))
            (do
              (.onNext response-observer (CreateResponse/getDefaultInstance))
              (.onCompleted response-observer)
              nil)
            (.onError response-observer (.asRuntimeException Status/ALREADY_EXISTS))))))

    (retrieve [^RetrieveRequest req ^StreamObserver response-observer]
      (locking store
        (let [key (.. req getKey asReadOnlyByteBuffer)]
          (simulate-work read-delay-millis)
          (if-let [^ByteBuffer value (.get ^HashMap store key)]
            (do
              (.onNext response-observer
                       (.. (RetrieveResponse/newBuilder)
                           (setValue (ByteString/copyFrom ^ByteBuffer (.slice value)))
                           build))
              (.onCompleted response-observer)
              nil)
            (.onError response-observer (.asRuntimeException Status/NOT_FOUND))))))

    (update [^UpdateRequest req ^StreamObserver response-observer]
      (locking store
        (let [key (.. req getKey asReadOnlyByteBuffer)
              value (.. req getValue asReadOnlyByteBuffer)]
          (simulate-work write-delay-millis)
          (if (.containsKey ^HashMap store key)
            (do
              (.replace ^HashMap store key value)
              (.onNext response-observer (UpdateResponse/getDefaultInstance))
              (.onCompleted response-observer)
              nil)
            (.onError response-observer (.asRuntimeException Status/NOT_FOUND))))))

    (delete [^DeleteRequest req ^StreamObserver response-observer]
      (locking store
        (let [key (.. req getKey asReadOnlyByteBuffer)]
          (simulate-work write-delay-millis)
          (.remove ^HashMap store key)
          (.onNext response-observer (DeleteResponse/getDefaultInstance))
          (.onCompleted response-observer))))))
