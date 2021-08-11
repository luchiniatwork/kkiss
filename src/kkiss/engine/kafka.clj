(ns kkiss.engine.kafka
  (:require [kkiss.engine :as engine]
            [kkiss.serde :as serde]
            [dvlopt.kafka :as kafka]
            [dvlopt.kafka.admin :as admin]
            [dvlopt.kafka.in :as in]
            [dvlopt.kafka.out :as out]
            [clojure.core.async :refer [<! >! go go-loop chan pub sub put!]]))

(def ^:private topic-name-serializer #(-> %
                                          ((serde/serializer :keyword))
                                          ((serde/deserializer :string))))

(def ^:private topic-name-deserializer #(-> %
                                            ((serde/serializer :string))
                                            ((serde/deserializer :keyword))))

(defmethod engine/create-engine :kafka [{:keys [streams conn config] :as opts}]
  (let [{:keys [nodes]} conn
        producer (out/producer {::kafka/nodes nodes
                                ::out/configuration config})]
    {:engine-id :kafka
     :streams streams
     :producer producer
     :conn conn
     :config config}))

(defn ^:private create-topic [{:keys [conn config streams]} stream]
  (println "creating topic")
  (let [{:keys [nodes]} conn
        {:keys [partitions replication]
         :or {partitions 1 replication 3}} (get streams stream)
        topic-name (topic-name-serializer stream)]
    (with-open [client (admin/admin {::admin/configuration config
                                     ::kafka/nodes nodes})]
      @(get (admin/create-topics client
                                 {topic-name
                                  {::admin/number-of-partitions partitions
                                   ::admin/replication-factor replication}})
            topic-name)
      )))

(defmethod engine/send! :kafka [{:keys [producer] :as engine} stream k v]
  (try
    @(out/send producer {::kafka/topic (topic-name-serializer stream)
                         ::kafka/key k
                         ::kafka/value v})
    (catch Throwable t
      (println "deu ruim")
      (if (= org.apache.kafka.common.errors.InvalidTopicException (type (.getCause t)))
        (do
          (create-topic engine stream)
          (println "came back, trying again")
          @(out/send producer {::kafka/topic (topic-name-serializer stream)
                               ::kafka/key k
                               ::kafka/value v}))
        (throw t)))))

(defmethod engine/consumer :kafka [{:keys [config conn] :as engine} opts streams handle-fn]
  (let [{:keys [nodes]} conn
        consumer (in/consumer {::kafka/nodes nodes
                               ::in/configuration (merge config opts)})]
    (in/register-for consumer (mapv topic-name-serializer streams))
    {:engine engine
     :streams streams
     :consumer consumer
     :handle-fn handle-fn
     :state (atom :stopped)}))

(defmethod engine/start! :kafka [{:keys [consumer handle-fn state] :as consumer}]
  (when (= :stopped @state)
    (go-loop []
      (println "polling...")
      (doseq [record (in/poll consumer
                              {::kafka/timeout [2 :seconds]})]
        #_(println (format "Record %d @%d - Key = %d, Value = %d"
                           (::kafka/offset record)
                           (::kafka/timestamp record)
                           (::kafka/key record)
                           (::kafka/value record)))
        (handle-fn (topic-name-deserializer (::kafka/topic record))
                   (::kafka/key record)
                   (::kafka/value record)))
      (when (= :running @state)
        (recur)))
    (reset! state :running))
  
  #_(when (= :stopped @state)
      (doseq [sub-chan sub-chans]
        (go-loop []
          (let [event (<! sub-chan)]
            (handle-fn (-> event :payload first)
                       (-> event :payload last)))
          (when (= :running @state)
            (recur))))
      (reset! state :running)))


(defmethod engine/stop! :kafka [{:keys [state] :as consumer}]
  (when (= :running @state)
    (reset! state :stopped)))
