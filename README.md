``` clojure
(def config {:implementation :in-memory})

(def streams
  {:my-topic1 {:key.serde (k/serde :keyword)
               :value.serde (k/serde :edn)}})

(def engine (k/engine {:streams streams
                       :config config}))

(k/send! engine
         :my-topic1
         :my-key {:foo :bar})

(def consumer
  (k/consumer engine {:group.id :a})
  [:my-topic1]
  (fn [k v] (println "key" k "; value" v)))


(k/start! consumer)

(k/pause! consumer)

(k/resume! consumer)

(k/stop! consumer)
```
