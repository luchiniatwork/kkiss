(ns kkiss.engine.kafka
  (:require [anomalies.core :as anom]
            [clojure.core.async :refer [thread] :as async]
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

(defn ^:private create-topic* [{:keys [engine ::partitions
                                       ::replication ::topic-config]
                                :as stream}]
  (let [{:keys [conn config]} engine
        {:keys [nodes]} conn
        topic-name (topic-name-serializer (:name stream))
        topic-config' (or topic-config {"retention.ms" "-1"} )]
    (with-open [client (admin/admin {::admin/configuration config
                                     ::kafka/nodes nodes})]
      (-> client
          (admin/create-topics
           (assoc {} topic-name
                  {::admin/number-of-partitions (or partitions 1)
                   ::admin/replication-factor (or replication 3)
                   ::admin/configuration.topic topic-config'}))
          (get topic-name)
          deref)
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
        (boolean (topics topic-name))))))

(defmethod engine/stream :kafka [{:keys [engine ::producer-config]
                                  :as opts}]
  (when-not (topic-exists? opts)
    (create-topic* opts))
  (let [{:keys [conn config]} engine
        {:keys [nodes]} conn
        producer (when producer-config
                   (out/producer {::kafka/nodes nodes
                                  ::out/configuration (merge config
                                                             producer-config)}))]
    (assoc opts :producer producer)))

(defmethod engine/send! :kafka [{:keys [producer] :as stream} k v]
  (when (nil? producer)
    (anom/throw-anom "Stream is not configured to produce."
                     {:category ::anom/incorrect}))
  (let [topic-name (topic-name-serializer (:name stream))]
    @(out/send producer {::kafka/topic topic-name
                         ::kafka/key k
                         ::kafka/value v})))

(defmethod engine/consumer :kafka [streams handle-fn
                                   {:keys [continuation
                                           ::commit-behavior]
                                    :or {commit-behavior :latest-before-failure}
                                    :as opts}]
  (when (and (= :latest-before-failure commit-behavior)
             (not= :stop-on-failure continuation))
    (anom/throw-anom (->> ["Continuation must be `:stop-on-failure`"
                           "when manual commit behavior is"
                           "`:latest-before-failure`"]
                          (s/join " "))
                     {:category ::anom/incorrect}))
  (let [engine (-> streams first :engine)
        {:keys [conn config]} engine
        {:keys [nodes]} conn
        consumer (in/consumer {::kafka/nodes nodes
                               ::in/configuration (merge config (::config opts))})]
    (in/register-for consumer (mapv #(-> % :name topic-name-serializer)
                                    streams))
    (assoc opts
           :engine engine
           :streams streams
           :consumer consumer
           :handle-fn handle-fn
           :state (atom :stopped))))

(defmethod engine/start! :kafka [{:keys [consumer handle-fn state
                                         ::polling-timeout
                                         ::commit-behavior]
                                  :or {polling-timeout 200
                                       commit-behavior :latest-before-failure}}]
  (when (= :stopped @state)
    (reset! state :running)
    (thread
      (loop []
        (let [last-partition (atom nil)
              last-offset (atom nil)]
          (doseq [record (in/poll consumer
                                  {::kafka/timeout [polling-timeout :milliseconds]})]
            (try
              (reset! last-partition
                      [(::kafka/topic record) (::kafka/partition record)])
              (reset! last-offset (::kafka/offset record))
              (handle-fn (::kafka/key record)
                         (::kafka/value record)
                         {:stream-name (topic-name-deserializer (::kafka/topic record))
                          :timestamp (::kafka/timestamp record)
                          ::topic (::kafka/topic record)
                          ::partition (::kafka/partition record)
                          ::offset (::kafka/offset record)})
              (catch Exception ex
                (when (= :latest-before-failure commit-behavior)
                  (in/commit-offsets consumer
                                     {::in/topic-partition->offset
                                      {@last-partition
                                       @last-offset}}))
                (throw ex)))))
        (when (= :latest-before-failure commit-behavior)
          (in/commit-offsets consumer))
        (if (= :running @state)
          (recur)
          (do (in/unregister consumer)
              (in/close consumer)))))))


(defmethod engine/stop! :kafka [{:keys [state] :as consumer}]
  (when (= :running @state)
    (reset! state :stopped)))
