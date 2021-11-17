(ns kkiss.engine
  (:require [anomalies.core :as anom]))

(defmulti create-engine (fn [{:keys [engine-id] :as opts}] engine-id))

(defmethod create-engine :default [_]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti stream (fn [{:keys [engine]}] (:engine-id engine)))

(defmethod stream :default [_]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti send! (fn [stream k v] (get-in stream [:engine :engine-id])))

(defmethod send! :default [_ _ _]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti consumer (fn [streams handle-fn opts] (-> streams first :engine :engine-id)))

(defmethod consumer :default [_ _ _]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti start! (fn [consumer] (-> consumer :streams first :engine :engine-id)))

(defmethod start! :default [consumer]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))

(defmulti stop! (fn [consumer] (-> consumer :streams first :engine :engine-id)))

(defmethod stop! :default [consumer]
  (anom/throw-anom "Not implemented" {::anom/category ::anom/incorrect}))
