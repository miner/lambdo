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
     (-db-reduce-kv db f3 init start-key reverse?)))

(defn reduce-keys
  ([f init db] (reduce-keys f init db nil))
  ([f init db start-key] (reduce-keys f init db start-key false))
  ([f init db start-key reverse?]
     (-db-reduce-keys db f init start-key reverse?)))

(defn transduce-db
  ([xform f init db] (transduce-db xform f init db nil))
  ([xform f init db start-key] (transduce-db xform f init db start-key false))
  ([xform f init db start-key reverse?]
     (-db-transduce db xform f init start-key reverse?)))


(defn keys-db
  ([db] (keys-db db nil))
  ([db start-key] (keys-db db start-key false))
  ([db start-key reverse?]
   (-db-reduce-keys db conj [] start-key reverse?)))


(defn next-key [db key]
  (reduce-keys (fn [_ k] (when (not= key k) (reduced k)))
             nil
             db
             key))

;; tricky way to get second, or first when key is nil
;; be careful about an empty database (that's what init is () and we take `first` on result)
(defn prev-key [db key]
  (first (reduce-keys (fn [res k]
                        (when (or (not key) (and (not res) (not= key k)))
                          (reduced (list k))))
                      ()
                      db
                      key
                      true)))

(defn first-key [db]
  (next-key db nil))

(defn last-key [db]
  (prev-key db nil))
