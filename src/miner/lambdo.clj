(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [miner.lambdo.impl :refer :all]
            [miner.lambdo.protocols :refer :all])
  (:import (miner.lambdo.impl Storage Bucket)
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
;; Also implements clojure.lang.Sorted
;; seq
;; subseq
;; rsubseq
;; 
;;
;; a database is a reducible -- in that it works as the "collection" final arg for a
;; 'reduce', 'reduce-kv' or 'transduce' call.  Slices can be used similarly with
;; reducible-keys and reducible-kvs.  Both take a db, then optional 'start-key' and optional
;; 'rev?' boolean.  Optional 'rev?' (boolean) determines if results are normal lexigraphical
;; by key (rev? = false, the default), or reverse lexigraphical (rev? = true).  If start-key
;; is given, results start at that key and end at the last or first key (as appropriate for
;; rev?).  A start-key of nil (the default) indicates first or last as appropriate for rev?.

(defn begin! [storage] (-begin! storage nil))

(defn commit! [storage] (-commit! storage))

(defn rollback! [storage] (-rollback! storage))

(defn open-bucket [storage bkey]
  (-open-bucket! storage bkey nil))

;; fill with optional snapshot
(defn create-bucket!
  ([storage bkey]
   (-open-bucket! storage bkey [DbiFlags/MDB_CREATE]))
  ([storage bkey snapshot]
   (let [bucket (create-bucket! storage bkey)]
     (begin! storage)
     ;; faster if snapshot is sorted and database starts empty
     ;; reduce-kv result is ignored, just for side-effect
     (if (sorted-snapshot? snapshot)
       (reduce-kv (fn [_ k v] (-append! bucket k v) nil) nil snapshot)
       (reduce-kv (fn [_ k v] (assoc! bucket k v) nil) nil snapshot))
     (commit! storage)
     ;; return opened bucket
     bucket)))

;; maybe call it bucket-slice ???
;; important: not a seq, just a reducible
;; although it is seqable so you can get the seq (paying for scanning)
;;
;; SEM: consider how this relates to a Clojure eduction.  Do you want to add an xform?  Not
;; really.

(defn reducible [bucket & {:keys [keys-only? start reverse?]}]
  (-reducible bucket keys-only? start reverse?))

;; same idea as contains? but implemented with a PKeyed protocol, and a less controversial name
(defn key? [bucket key]
  (-key? bucket key))

(defn next-key [bucket key] (-next-key bucket key))

(defn previous-key [bucket key] (-previous-key bucket key))

(defn first-key [bucket] (-first-key bucket))

(defn last-key [bucket] (-last-key bucket))

;; returns plain hash-map (not sorted)
;; see also persistent! which returns sorted-map for better consistency with bucket
(defn snapshot [bucket] (persistent! (reduce-kv assoc! (transient {}) bucket)))

