(ns kkiss.core
  (:require [hyperfiddle.rcf :refer [tests]]
            [kkiss.engine :as engine]
            [^:keep kkiss.engine.in-memory]))

(defn engine [{:keys [streams config] :as opts}]
  (engine/create-engine opts))

(defn send! [engine stream k v]
  (engine/send! engine stream k v))

(defn consumer [engine opts streams handle-fn]
  (engine/consumer engine opts streams handle-fn))

(defn start! [consumer]
  (engine/start! consumer))

(defn stop! [consumer]
  (engine/stop! consumer))


(tests
  (let [streams {:my-stream-a {:key.serde (kkiss.serde/serde :keyword)
                               :value.serde (kkiss.serde/serde :keyword)}
                 :my-stream-b {:key.serde (kkiss.serde/serde :keyword)
                               :value.serde (kkiss.serde/serde :keyword)}}
        e (engine {:config {:engine-id :in-memory}
                   :streams streams})
        c1-visited (atom 0)
        c2-visited (atom 0)
        c1 (consumer e {} [:my-stream-a] (fn [k v]
                                           (when (and (= k :foo)
                                                      (= v :bar))
                                             (swap! c1-visited inc))))
        c2 (consumer e {} [:my-stream-a
                           :my-stream-b] (fn [k v]
                                           (when (and (= k :foo)
                                                      (= v :bar))
                                             (swap! c2-visited inc))))]
    (start! c1)
    (start! c2)
    (send! e :my-stream-a :foo :bar)
    (send! e :my-stream-b :foo :bar)
    (Thread/sleep 50)
    [@c1-visited @c2-visited]) := [1 2]


  )




(comment
  (def e (engine {:config {:engine-id :in-memory}}))

  (def c1 (consumer e {} [:foo :bar] (fn [k v] (println "for c1 - k" k "v" v))))

  (def c2 (consumer e {} [:foo :ball] (fn [k v] (println "for c2 - k" k "v" v))))


  )
