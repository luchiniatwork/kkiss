(ns kkiss.engine
  (:require [anomalies.core :as anom]))

(defmulti create-engine (fn [{:keys [engine-id] :as opts}] engine-id))

(defmethod create-engine :default [_]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti send! (fn [engine stream k v] (:engine-id engine)))

(defmethod send! :default [engine _ _ _]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti consumer (fn [engine opts streams handle-fn] (:engine-id engine)))

(defmethod consumer :default [engine _ _ _]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti start! (fn [consumer] (-> consumer :engine :engine-id)))

(defmethod start! :default [consumer]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti stop! (fn [consumer] (-> consumer :engine :engine-id)))

(defmethod stop! :default [consumer]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))
