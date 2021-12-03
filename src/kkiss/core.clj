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

(defn consumer
  ([streams handle-fn]
   (consumer streams handle-fn nil))
  ([streams handle-fn opts]
   (let [wrapper (fn [stream-name k v]
                   (let [k-de (-> streams (find-stream stream-name)
                                  (get-in [:key.serde :deserializer]))
                         v-de (-> streams (find-stream stream-name)
                                  (get-in [:value.serde :deserializer]))]
                     (try
                       (handle-fn stream-name (k-de k) (v-de v))
                       (catch Throwable ex
                         (println "OPA!!! erro aqui" ex)
                         (comment "ignore exception for now")))))]
     (engine/consumer streams wrapper opts))))

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
         c1 (consumer [stream-a] (fn [s-name k v]
                                   (when (and (= k :foo)
                                              (= v :bar))
                                     (swap! c1-visited inc))))
         c2 (consumer [stream-a stream-b] (fn [_ k v]
                                            (when (and (= k :foo)
                                                       (= v :bar))
                                              (swap! c2-visited inc))))]
     (start! c1)
     (start! c2)
     (send! stream-a :foo :bar)
     (send! stream-b :foo :bar)
     (Thread/sleep 50)
     [@c1-visited @c2-visited]) := [1 2]

   "throw does not break consumer"
   (let [e (engine {:engine-id :in-memory})
         stream-a (stream {:engine e
                           :name :my-stream-a
                           :key.serde (serde/serde :keyword)
                           :value.serde (serde/serde :keyword)})
         c-visited (atom 0)
         c (consumer [stream-a] (fn [_ k v]
                                  (if (and (= k :foo)
                                           (= v :bar))
                                    (swap! c-visited inc)
                                    (throw (ex-info "invalid msg" {})))))]
     (start! c)
     (send! stream-a :foo :bar)
     (send! stream-a :foo :ball)
     (send! stream-a :foo :bar)
     (Thread/sleep 50)
     @c-visited) := 2

   
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

  (send! test-stream :k :v6)
  
  (def c1 (consumer [test-stream]
                    (fn [stream k v] (println stream k v))
                    {:config {"auto.offset.reset" "earliest"
                              "enable.auto.commit" true
                              "group.id"           "my-group1"}}))

  (start! c1)

  (stop! c1)

  )
