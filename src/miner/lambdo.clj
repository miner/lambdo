(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [miner.lambdo.impl :refer :all]
            [miner.lambdo.protocols :refer :all])
  (:import (miner.lambdo.impl Storage Database)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags)))




;; SEM FIXME: do something with options,  `create` ignored

(defn open-storage ^Storage [dirpath & {:keys [size-mb create]}]
  (let [^Env env (create-env dirpath (or size-mb 10))]
    (->Storage dirpath
               env
               nil
               (doto (.txnRead env) (.reset)))))

(defn create-storage! ^Storage [dirpath & {:keys [size-mb]}]
  (let [base (io/file dirpath)
        filepath (if (= (.getName base) ".") base (io/file base "."))]
    (io/make-parents filepath)
    (open-storage filepath :size-mb size-mb :create true)))

(defn close-storage! [^Storage storage]
  (io!)
  (.close storage)
  storage)


;; fetch is now get
;; store! is now assoc!

(defn open-database [storage dbkey]
  (-open-database! storage dbkey nil))

(defn create-database! [storage dbkey]
  (-open-database! storage dbkey [DbiFlags/MDB_CREATE]))

(defn begin! [storage] (-begin! storage nil))

(defn commit! [storage] (-commit! storage))

(defn rollback! [storage] (-rollback! storage))

;; db-slice db-subseq select
(defn reducible-kvs
  ([db] (reducible-kvs db nil))
  ([db start-key] (reducible-kvs db start-key false))
  ([db start-key rev?]
   (-reducible-kvs db start-key rev?)))

;; naming key-range, db-keys
(defn reducible-keys
  ([db] (reducible-keys db nil))
  ([db start-key] (reducible-keys db start-key false))
  ([db start-key rev?]
   (-reducible-keys db start-key rev?)))


;; same idea as contains? but implemented with a PKeyed protocol, and a less controversial name
(defn has-key? [db key]
  (-has-key? db key))

(defn next-key [db key] (-next-key db key))

(defn previous-key [db key] (-previous-key db key))

(defn first-key [db] (-first-key db))

(defn last-key [db] (-last-key db))
