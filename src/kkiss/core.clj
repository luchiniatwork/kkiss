(ns kkiss.core
  (:require #_[hyperfiddle.rcf :refer [tests]]
            [kkiss.engine :as engine]
            [kkiss.serde :as serde]
            [^:keep kkiss.engine.in-memory]
            [^:keep kkiss.engine.kafka]))

(defn engine [{:keys [config] :as opts}]
  (engine/create-engine opts))

(defn stream [opts]
  (engine/stream opts))

(defn send! [stream k v]
  (let [k-ser (get-in stream [:key.serde :serializer])
        v-ser (get-in stream [:value.serde :serializer])]
    (engine/send! stream (k-ser k) (v-ser v))))

(defn ^:private find-stream [streams stream-name]
  (->> streams
       (filter #(= stream-name (:name %)))
       first))

(defn ^:private arity-3? [f]
  (let [members (-> f
                    clojure.reflect/reflect
                    :members)
        arity-3? (or (= 3 (->> members
                               (filter #(= 'invoke (:name %)))
                               first
                               :parameter-types
                               count))
                     (->> members
                          (filter #(= 'doInvoke (:name %)))
                          seq))]
    arity-3?))

(defn consumer
  ([streams handle-fn]
   (consumer streams handle-fn nil))
  ([streams handle-fn {:keys [continuation
                              failed-event-stream
                              failed-exception-stream]
                       :or {continuation :stop-on-failure}
                       :as opts}]
   (let [arity-3?' (arity-3? handle-fn)
         wrapper (fn [k v {:keys [stream-name] :as opts}]
                   (let [k-de (-> streams (find-stream stream-name)
                                  (get-in [:key.serde :deserializer]))
                         v-de (-> streams (find-stream stream-name)
                                  (get-in [:value.serde :deserializer]))]
                     (try
                       (if arity-3?'
                         (handle-fn (k-de k) (v-de v) opts)
                         (handle-fn (k-de k) (v-de v)))
                       (catch Throwable ex
                         (when failed-event-stream
                           (send! failed-event-stream (k-de k) (v-de v)))
                         (when failed-exception-stream
                           (send! failed-exception-stream
                                  (k-de k)
                                  {:event (v-de v)
                                   :exception (Throwable->map ex)}))
                         (if (= :non-stop continuation)
                           (println "Ignored exception" ex)
                           (throw ex))))))]
     (engine/consumer streams wrapper (assoc opts
                                             :continuation continuation)))))

(defn start! [consumer]
  (engine/start! consumer))

(defn stop! [consumer]
  (engine/stop! consumer))




#_(tests
   "several consumers"
   (let [e (engine {:engine-id :in-memory})
         stream-a (stream {:engine e
                           :name :my-stream-a
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         stream-b (stream {:engine e
                           :name :my-stream-b
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         c1-visited (atom 0)
         c2-visited (atom 0)
         c1 (consumer [stream-a] (fn [k v {:keys [foobar]}]
                                   (when (and (= k :foo)
                                              (= v :bar))
                                     (swap! c1-visited inc))))
         c2 (consumer [stream-a stream-b] (fn [k v]
                                            (when (and (= k :foo)
                                                       (= v :bar))
                                              (swap! c2-visited inc))))]
     (start! c1)
     (start! c2)
     (send! stream-a :foo :bar)
     (send! stream-b :foo :bar)
     (Thread/sleep 50)
     [@c1-visited @c2-visited]) := [1 2]


   "throw breaks consumer by default"
   (let [e (engine {:engine-id :in-memory})
         stream-a (stream {:engine e
                           :name :my-stream-a
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         c-visited (atom 0)
         c (consumer [stream-a] (fn [k v]
                                  (if (and (= k :foo)
                                           (= v :bar))
                                    (swap! c-visited inc)
                                    (throw (ex-info "invalid msg" {})))))]
     (start! c)
     (send! stream-a :foo :bar)
     (send! stream-a :foo :ball)
     (send! stream-a :foo :bar)
     (Thread/sleep 50)
     @c-visited) := 1


   "throw does not stops consumer in case the consumer wants it"
   (let [e (engine {:engine-id :in-memory})
         stream-a (stream {:engine e
                           :name :my-stream-a
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         c-visited (atom 0)
         c (consumer [stream-a] (fn [k v]
                                  (if (and (= k :foo)
                                           (= v :bar))
                                    (swap! c-visited inc)
                                    (throw (ex-info "invalid msg" {}))))
                     {:continuation :non-stop})]
     (start! c)
     (send! stream-a :foo :bar)
     (send! stream-a :foo :ball)
     (send! stream-a :foo :bar)
     (Thread/sleep 50)
     @c-visited) := 2


   "throw does not break consumer but also send event to another
   stream and exception to another one"
   (let [e (engine {:engine-id :in-memory})
         stream-a (stream {:engine e
                           :name :my-stream-a
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         failed-stream (stream {:engine e
                                :name :failed-stream
                                :key.serde (serde/serde :keyword)
                                :value.serde (serde/serde :keyword)})
         ex-stream (stream {:engine e
                            :name :ex-stream
                            :key.serde (serde/serde :keyword)
                            :value.serde (serde/serde :keyword)})
         c-visited (atom 0)
         c (consumer [stream-a] (fn [k v]
                                  (if (and (= k :foo)
                                           (= v :bar))
                                    (swap! c-visited inc)
                                    (throw (ex-info "invalid msg" {}))))
                     {:continuation :non-stop
                      :failed-event-stream failed-stream
                      :failed-exception-stream ex-stream})
         failed-c-out (atom {})
         failed-c (consumer [failed-stream ex-stream]
                            (fn [k v {:keys [stream-name]}]
                              (swap! failed-c-out assoc
                                     stream-name {:k k
                                                  :result v})))]
     (start! c)
     (start! failed-c)
     (send! stream-a :foo :bar)
     (send! stream-a :foo :ball)
     (send! stream-a :foo :bar)
     (Thread/sleep 50)
     [@c-visited
      (= :ball (-> @failed-c-out :failed-stream :result))
      (= :ball (-> @failed-c-out :ex-stream :result :event))
      (-> @failed-c-out :ex-stream :result :exception :cause)]) := [2 true true "invalid msg"]
   )




(comment

  (def base-config {"bootstrap.servers" "pkc-419q3.us-east4.gcp.confluent.cloud:9092"
                    "security.protocol" "SASL_SSL"
                    "sasl.jaas.config" "org.apache.kafka.common.security.plain.PlainLoginModule required username='PSAYAEOPNHGZMGV3' password='FTJaUu4d09b1kfDFidIKZKJu8FHQ/NedaN+IdnovmJ2O6FwOrfnys+Ku1nUQeJj9';"
                    "sasl.mechanism" "PLAIN"
                    "client.dns.lookup" "use_all_dns_ips"})

  (def e (engine {:engine-id :kafka
                  :conn {:nodes [["pkc-419q3.us-east4.gcp.confluent.cloud" 9092]]}
                  :config base-config}))

  (def test-stream (stream {:name :test-stream
                            :engine e
                            :key.serde (serde/serde :keyword)
                            :value.serde (serde/serde :keyword)
                            :partitions 1
                            :replication 3
                            :config {"client.id" "my-producer"
                                     "acks" "all"}}))

  (send! test-stream :k :v677)
  
  (def c3 (consumer [test-stream]
                    (fn [k v] (println k v))
                    {:polling-timeout 2000
                     :config {"auto.offset.reset" "earliest"
                              "enable.auto.commit" true
                              "group.id"           "my-group4"}}))

  (start! c3)

  (stop! c3)

;;;; exception

  ;; never commits
  (def c4 (consumer [test-stream]
                    (fn [k v]
                      (println k v))
                    {:polling-timeout 2000
                     :kkiss.engine.kafka/commit-behavior :do-not-manually-commit
                     :config {"auto.offset.reset" "earliest"
                              "enable.auto.commit" false
                              "group.id"           "my-group6"}}))

  (start! c4)

  (stop! c4)


  (def c5 (consumer [test-stream]
                    (fn [k v]
                      (when (= :v7777 v)
                        (throw (ex-info "what is this?" {})))
                      (println k v))
                    {:polling-timeout 200
                     :config {"auto.offset.reset" "earliest"
                              "enable.auto.commit" false
                              "group.id"           "my-group15"}}))

  (start! c5)

  (stop! c5)

  
  )
