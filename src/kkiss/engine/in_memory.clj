(ns kkiss.engine.in-memory
  (:require [kkiss.engine :as engine]
            [clojure.core.async :refer [<! >! go go-loop chan pub sub put!]]))

(defmethod engine/create-engine :in-memory [{:keys [streams config] :as opts}]
  (let [in-chan (chan 1024)]
    {:engine-id :in-memory
     :streams streams
     :in-chan in-chan
     :stream-pub (pub in-chan :stream)}))

(defmethod engine/send! :in-memory [{:keys [in-chan] :as engine} stream k v]
  (put! in-chan {:stream stream
                 :payload [k v]}))

(defmethod engine/consumer :in-memory [{:keys [stream-pub] :as engine} opts streams handle-fn]  
  (let [sub-chans (mapv (fn [stream]
                          (let [sub-chan (chan 256)]
                            (sub stream-pub stream sub-chan)
                            sub-chan))
                        streams)]
    {:engine engine
     :streams streams
     :sub-chans sub-chans
     :handle-fn handle-fn
     :state (atom :stopped)}))

(defmethod engine/start! :in-memory [{:keys [sub-chans handle-fn state] :as consumer}]
  (when (= :stopped @state)
    (doseq [sub-chan sub-chans]
      (go-loop []
        (let [{:keys [stream payload] :as event} (<! sub-chan)]
          (handle-fn stream
                     (first payload)
                     (second payload)))
        (when (= :running @state)
          (recur))))
    (reset! state :running)))


(defmethod engine/stop! :in-memory [{:keys [sub-chans handle-fn state] :as consumer}]
  (when (= :running @state)
    (reset! state :stopped)))
