(ns kkiss.core
  (:require #_[hyperfiddle.rcf :refer [tests]]
            [kkiss.engine :as engine]
            [kkiss.serde :as serde]
            [^:keep kkiss.engine.in-memory]
            [^:keep kkiss.engine.kafka]))

(defn engine [{:keys [streams config] :as opts}]
  (engine/create-engine opts))

(defn send! [engine stream k v]
  (let [k-ser (get-in engine [:streams stream :key.serde :serializer])
        v-ser (get-in engine [:streams stream :value.serde :serializer])]
    (engine/send! engine stream (k-ser k) (v-ser v))))

(defn consumer [engine opts streams handle-fn]
  (let [wrapper (fn [stream k v]
                  (let [k-de (get-in engine [:streams stream :key.serde :deserializer])
                        v-de (get-in engine [:streams stream :value.serde :deserializer])]
                    (try
                      (handle-fn stream (k-de k) (v-de v))
                      (catch Throwable _
                        (comment "ignore exception for now")))))]
    (engine/consumer engine opts streams wrapper)))

(defn start! [consumer]
  (engine/start! consumer))

(defn stop! [consumer]
  (engine/stop! consumer))


#_(tests
    "several consumers"
    (let [streams {:my-stream-a {:key.serde (serde/serde :keyword)
                                 :value.serde (serde/serde :keyword)}
                   :my-stream-b {:key.serde (serde/serde :keyword)
                                 :value.serde (serde/serde :keyword)}}
          e (engine {:engine-id :in-memory
                     :streams streams})
          c1-visited (atom 0)
          c2-visited (atom 0)
          c1 (consumer e {} [:my-stream-a] (fn [_ k v]
                                             (when (and (= k :foo)
                                                        (= v :bar))
                                               (swap! c1-visited inc))))
          c2 (consumer e {} [:my-stream-a
                             :my-stream-b] (fn [_ k v]
                                             (when (and (= k :foo)
                                                        (= v :bar))
                                               (swap! c2-visited inc))))]
      (start! c1)
      (start! c2)
      (send! e :my-stream-a :foo :bar)
      (send! e :my-stream-b :foo :bar)
      (Thread/sleep 50)
      [@c1-visited @c2-visited]) := [1 2]

    "throw does not break consumer"
    (let [streams {:my-stream-a {:key.serde (serde/serde :keyword)
                                 :value.serde (serde/serde :keyword)}}
          e (engine {:engine-id :in-memory
                     :streams streams})
          c-visited (atom 0)
          c (consumer e {} [:my-stream-a] (fn [_ k v]
                                            (if (and (= k :foo)
                                                     (= v :bar))
                                              (swap! c-visited inc)
                                              (throw (ex-info "invalid msg" {})))))
          ]
      (start! c)
      (send! e :my-stream-a :foo :bar)
      (send! e :my-stream-a :foo :ball)
      (send! e :my-stream-a :foo :bar)
      (Thread/sleep 50)
      @c-visited) := 2

    
    )




(comment
  (def e (engine {:engine-id :in-memory
                  :streams {:foo {:key.serde (serde/serde :keyword)
                                  :value.serde (serde/serde :keyword)}}}))

  (send! e :foo :k1 :v1)
  
  (def c1 (consumer e {} [:foo :bar] (fn [_ k v] (println "for c1 - k" k "v" v))))

  (def c2 (consumer e {} [:foo :ball] (fn [_ k v] (println "for c2 - k" k "v" v))))
  
  )


(comment

  (def base-config {"bootstrap.servers" "pkc-419q3.us-east4.gcp.confluent.cloud:9092"
                    "security.protocol" "SASL_SSL"
                    "sasl.jaas.config" "org.apache.kafka.common.security.plain.PlainLoginModule required username='PSAYAEOPNHGZMGV3' password='FTJaUu4d09b1kfDFidIKZKJu8FHQ/NedaN+IdnovmJ2O6FwOrfnys+Ku1nUQeJj9';"
                    "sasl.mechanism" "PLAIN"
                    "client.dns.lookup" "use_all_dns_ips"})

  (def e (engine {:engine-id :kafka
                  :streams {:test-stream {:key.serde (serde/serde :keyword)
                                          :value.serde (serde/serde :keyword)
                                          :partitions 1
                                          :replication 3}}
                  :conn {:nodes [["pkc-419q3.us-east4.gcp.confluent.cloud" 9092]]}
                  :config (merge base-config
                                 {"client.id" "my-producer"
                                  "acks" "all"})}))

  (send! e :test-stream :k :v)
  
  (def c1 (consumer e {"auto.offset.reset" "earliest"
                       "enable.auto.commit" true
                       "group.id"           "my-group"}
                    [:test-stream]
                    (fn [stream k v] (println stream k v))))

  )
