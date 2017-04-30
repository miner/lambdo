(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [miner.lambdo.impl :refer :all]
            [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (miner.lambdo.impl Storage Database)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags)))




;; SEM FIXME: do something with options,  create ignored

(defn open-storage ^Storage [dirpath & {:keys [size-mb create]}]
  (let [^Env env (create-env dirpath (or size-mb 10))]
    (->Storage dirpath
               env
               nil
               (doto (.txnRead env) (.reset)))))

(defn create-storage! ^Storage [dirpath & {:keys [size-mb]}]
  (open-storage dirpath :size-mb size-mb :create true))

(defn close-storage! [^Storage storage]
  (io!)
  (.close storage)
  storage)

(defn fetch [db key] (-fetch db key))

(defn store! [db key val] (-store! db key val))

(defn open-database [storage dbkey]
  (-open-database! storage dbkey nil))

(defn create-database! [storage dbkey]
  (-open-database! storage dbkey [DbiFlags/MDB_CREATE]))

(defn begin! [storage] (-begin! storage nil))

(defn commit! [storage] (-commit! storage))

(defn rollback! [storage] (-rollback! storage))


(defn reduce-db
  ([f3 init db] (reduce-db f3 init db nil))
  ([f3 init db start-key] (reduce-db f3 init db start-key false))
  ([f3 init db start-key reverse?]
     (-db-reduce db f3 init start-key reverse?)))


(defn transduce-db
  ([xform f init db] (transduce-db xform f init db nil))
  ([xform f init db start-key] (transduce-db xform f init db start-key false))
  ([xform f init db start-key reverse?]
     (-db-transduce db xform f init start-key reverse?)))


(defn keys-db
  ([db] (keys-db db nil))
  ([db start-key] (keys-db db start-key false))
  ([db start-key reverse?]
   (-db-keys db start-key reverse?)))

