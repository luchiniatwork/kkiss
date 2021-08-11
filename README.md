``` clojure
(def engine (k/engine {:engine-id :in-memory
                       :streams {:foo {:key.serde (serde/serde :keyword)
                                       :value.serde (serde/serde :keyword)}}}))

(k/send! engine
         :foo
         :my-key {:foo-key1 :bar-value1
                  :foo-key2 :bar-value2})

(def consumer
  (k/consumer engine {:group.id :a})
  [:foo]
  (fn [s k v] (println "stream" s "key" k " value" v)))


(k/start! consumer)

(k/stop! consumer)
```
