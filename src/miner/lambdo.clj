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


;; a Database is accessed like an ITransientMap.
;; Important note: all LMDB requires user to guarantee thread isolation.  Clojure transients
;; are like that, too.
;;
;; Bash-in-place actually works, but I'm not sure I should encourage it.
;; Not good Clojure style, but simpler to read -- still considering.  Of course, Clojure
;; style will work.
;;
;; get  -- also keyword access works (:x db)
;; assoc!
;; dissoc!
;;
;; a database is a reducible -- in that it works as the "collection" final arg for a
;; 'reduce', 'reduce-kv' or 'transduce' call.  Slices can be used similarly with
;; reducible-keys and reducible-kvs.  Both take a db, then optional 'start-key' and optional
;; 'rev?' boolean.  Optional 'rev?' (boolean) determines if results are normal lexigraphical
;; by key (rev? = false, the default), or reverse lexigraphical (rev? = true).  If start-key
;; is given, results start at that key and end at the last or first key (as appropriate for
;; rev?).  A start-key of nil (the default) indicates first or last as appropriate for rev?.

(defn open-database [storage dbkey]
  (-open-database! storage dbkey nil))

(defn create-database! [storage dbkey]
  (-open-database! storage dbkey [DbiFlags/MDB_CREATE]))

(defn begin! [storage] (-begin! storage nil))

(defn commit! [storage] (-commit! storage))

(defn rollback! [storage] (-rollback! storage))

(defn reducible [db & {:keys [keys-only? start reverse?]}]
  (-reducible db keys-only? start reverse?))

;; same idea as contains? but implemented with a PKeyed protocol, and a less controversial name
(defn has-key? [db key]
  (-has-key? db key))

(defn next-key [db key] (-next-key db key))

(defn previous-key [db key] (-previous-key db key))

(defn first-key [db] (-first-key db))

(defn last-key [db] (-last-key db))
