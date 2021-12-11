(ns kkiss.engine.in-memory
  (:require [kkiss.engine :as engine]
            [clojure.core.async :refer [<! >! go go-loop chan pub sub put!]]))

(defmethod engine/create-engine :in-memory [{:keys [config] :as opts}]
  {:engine-id :in-memory})

(defmethod engine/stream :in-memory [{:keys [engine] :as opts}]
  (let [in-chan (chan 1024)]
    (assoc opts
           :in-chan in-chan
           :stream-pub (pub in-chan :stream-name))))

(defmethod engine/send! :in-memory [{:keys [in-chan] :as stream} k v]
  (put! in-chan {:stream-name (:name stream)
                 :stream-payload [k v]}))

(defmethod engine/consumer :in-memory
  [streams handle-fn opts]
  (let [sub-chans (mapv (fn [{:keys [stream-pub] :as stream}]
                          (let [sub-chan (chan 1024)]
                            (sub stream-pub (:name stream) sub-chan)
                            sub-chan))
                        streams)]
    {:streams streams
     :sub-chans sub-chans
     :handle-fn handle-fn
     :state (atom :stopped)}))

(defmethod engine/start! :in-memory [{:keys [sub-chans handle-fn state] :as consumer}]
  (when (= :stopped @state)
    (reset! state :running)
    (doseq [sub-chan sub-chans]
      (go-loop []
        (let [{:keys [stream-name stream-payload] :as event} (<! sub-chan)]
          (handle-fn (first stream-payload)
                     (second stream-payload)
                     {:stream-name stream-name}))
        (when (= :running @state)
          (recur))))))


(defmethod engine/stop! :in-memory [{:keys [sub-chans handle-fn state] :as consumer}]
  (when (= :running @state)
    (reset! state :stopped)))
