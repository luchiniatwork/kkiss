(ns kkiss.engine.kafka
  (:require [clojure.core.async :refer [<! >! go go-loop chan pub sub put!]]
            [clojure.string :as s]
            [dvlopt.kafka :as kafka]
            [dvlopt.kafka.admin :as admin]
            [dvlopt.kafka.in :as in]
            [dvlopt.kafka.out :as out]
            [kkiss.engine :as engine]
            [kkiss.serde :as serde]))

(def ^:private topic-name-serializer #(-> %
                                          ((serde/serializer :keyword))
                                          ((serde/deserializer :string))
                                          (s/replace-first #"^:" "")
                                          (s/replace-first #"\/" "__")))

(def ^:private topic-name-deserializer #(-> %
                                            (s/replace-first #"__" "/")
                                            keyword))

(defmethod engine/create-engine :kafka [{:keys [conn config] :as opts}]
  {:engine-id :kafka
   :conn conn
   :config config})

(defn ^:private create-topic* [{stream-config :config
                                :keys [ engine partitions replication]
                                :or {partitions 1 replication 3}
                                :as stream}]
  (let [{:keys [conn config]} engine
        {:keys [nodes]} conn
        topic-name (topic-name-serializer (:name stream))]
    (with-open [client (admin/admin {::admin/configuration config
                                     ::kafka/nodes nodes})]
      (admin/create-topics
       client
       (assoc {} topic-name
              (merge stream-config
                     {::admin/number-of-partitions partitions
                      ::admin/replication-factor replication})))
      topic-name)))

(defn ^:private topic-exists? [{:keys [engine]
                                :as stream}]
  (let [{:keys [conn config]} engine
        {:keys [nodes]} conn
        topic-name (topic-name-serializer (:name stream))]
    (with-open [client (admin/admin {::admin/configuration config
                                     ::kafka/nodes nodes})]
      (let [topics (-> client
                       (admin/topics {::kafka/internal? false})
                       deref
                       keys
                       set)]
        (boolean (topics topic-name))) )))

(defmethod engine/stream :kafka [{stream-config :config
                                  :keys [engine]
                                  :as opts}]
  (when-not (topic-exists? opts)
    (create-topic* opts))
  (let [{:keys [conn config]} engine
        {:keys [nodes]} conn
        producer (out/producer {::kafka/nodes nodes
                                ::out/configuration (merge config
                                                           stream-config)})]
    (assoc opts :producer producer)))

(defmethod engine/send! :kafka [{:keys [producer] :as stream} k v]
  (let [topic-name (topic-name-serializer (:name stream))
        send-fn (fn [] @(out/send producer {::kafka/topic topic-name
                                            ::kafka/key k
                                            ::kafka/value v}))]
    (try
      (send-fn)
      (catch Throwable t
        (println "deu ruim")))))

(defmethod engine/consumer :kafka [streams handle-fn opts]
  (let [engine (-> streams first :engine)
        {:keys [conn config]} engine
        {:keys [nodes]} conn
        consumer (in/consumer {::kafka/nodes nodes
                               ::in/configuration (merge config (:config opts))})]
    (in/register-for consumer (mapv #(-> % :name topic-name-serializer)
                                    streams))
    {:engine engine
     :streams streams
     :consumer consumer
     :handle-fn handle-fn
     :state (atom :stopped)}))

(defmethod engine/start! :kafka [{:keys [consumer handle-fn state]
                                  :as consumer}]
  (when (= :stopped @state)
    (go-loop []
      (println "polling...")
      (doseq [record (in/poll consumer
                              {::kafka/timeout [2 :seconds]})]
        #_(println (::kafka/offset record)
                   (::kafka/timestamp record)
                   (::kafka/key record)
                   (::kafka/value record))
        (handle-fn (topic-name-deserializer (::kafka/topic record))
                   (::kafka/key record)
                   (::kafka/value record)))
      (when (= :running @state)
        (recur)))
    (reset! state :running)))


(defmethod engine/stop! :kafka [{:keys [state] :as consumer}]
  (when (= :running @state)
    (reset! state :stopped)))
