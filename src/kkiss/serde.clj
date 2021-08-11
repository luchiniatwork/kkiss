(ns kkiss.serde
  (:require [clojure.string        :as s]
            [edamame.core          :as edamame]
            #_[hyperfiddle.rcf       :refer [tests]]
            [jsonista.core         :as json]
            [jsonista.tagged       :as jt]))

(defmulti serializer (fn [kind & _] kind))

(defmulti deserializer (fn [kind & _] kind))

(defmulti serde (fn [kind & _] kind))


;; :string

(defmethod serializer :string [_ & _]
  (fn [^String v] (some-> v .getBytes)))

(defmethod deserializer :string [_ & _]
  (fn [^bytes bytes] (String. bytes java.nio.charset.StandardCharsets/UTF_8)))


;; edn

(defmethod serializer :edn [_ & _]
  (let [ser (serializer :string)]
    (fn [v] (some-> v pr-str ser))))

(defmethod deserializer :edn [_ & args]
  (let [de (deserializer :string)
        opts (first args)]
    (fn [^bytes bytes]
      (when bytes
        (edamame/parse-string ((deserializer :string) bytes)
                              opts)))))


;; :keyword

(defmethod serializer :keyword [_ & _]
  (let [ser (serializer :edn)]
    (fn [^clojure.lang.Keyword v] (ser v))))

(defmethod deserializer :keyword [_ & _]
  (let [de (deserializer :edn)]
    (fn [^bytes bytes] (de bytes))))


;; json

(defmethod serializer :json [_ & args]
  (if-let [mapper (first args)]
    (fn [v] (some-> v (json/write-value-as-bytes mapper)))
    (fn [v] (some-> v (json/write-value-as-bytes)))))

(defmethod deserializer :json [_ & args]
  (if-let [mapper (first args)]
    (fn [v] (some-> v (json/read-value mapper)))
    (fn [v] (some-> v (json/read-value)))))



(defmethod serde :default [kind]
  {:serializer (serializer kind)
   :deserializer (deserializer kind)})


#_(tests

   (tests
    ((deserializer :string) ((serializer :string) "Hello")) := "Hello")

   (tests
    ((deserializer :keyword) ((serializer :keyword) :foobar)) := :foobar
    ((deserializer :keyword) ((serializer :keyword) :foo/bar)) := :foo/bar)

   (tests
    ((deserializer :edn) ((serializer :edn) {:a :b})) := {:a :b}
    ((deserializer :edn) ((serializer :edn) {:foo/a 'b
                                             "vals" ['qwe/rty
                                                     '(1 4.5)]})) := {:foo/a 'b
                                                                      "vals" ['qwe/rty
                                                                              '(1 4.5)]}
    ((deserializer :edn {:fn true}) ((serializer :string) "#(* % %1 %2)")) := '(fn* [%1 %2] (* %1 %1 %2)))

   (tests
    ((deserializer :json) ((serializer :json) {"a" "b"})) := {"a" "b"}
    (let [mapper (json/object-mapper
                  {:encode-key-fn true
                   :decode-key-fn true})]
      ((deserializer :json mapper) ((serializer :json mapper) {:a "b"}))) := {:a "b"}
    (let [mapper (json/object-mapper
                  {:encode-key-fn true
                   :decode-key-fn true
                   :modules [(jt/module
                              {:handlers {clojure.lang.Keyword {:tag "!kw"
                                                                :encode jt/encode-keyword
                                                                :decode keyword}
                                          clojure.lang.PersistentHashSet {:tag "!set"
                                                                          :encode jt/encode-collection
                                                                          :decode set}}})]})]
      ((deserializer :json mapper) ((serializer :json mapper)
                                    {:system/status #{:status/good}}))) := {:system/status #{:status/good}})

   (tests

    (let [bytes (.getBytes ":foo/bar")
          {:keys [serializer deserializer]} (serde :keyword)]
      [(String. (serializer :foo/bar))
       (deserializer bytes)]) := [":foo/bar" :foo/bar]
    )


   )
