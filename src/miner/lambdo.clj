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


;; a Bucket is accessed like an ITransientMap.
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
;; a Bucket is a "reducible" -- in that it works as the "collection" final arg for a
;; 'reduce', 'reduce-kv' or 'transduce' call.  The bucket is naturally Sorted (as if by
;; pr-compare) so the keys and entries are always visited in pr-compare order.
;;
;; Slices of the bucket can be specified with `reducible` which takes a bucket, then
;; optional :keys-only? bool, :start start-key, and :reverse? bool.  Optional :reverse?
;; (boolean) determines if results are in lexigraphical order by key (:reverse? false, the
;; default), or lexigraphically reverse order.  If :start is given, results start at that key and
;; end at the last or first key (as appropriate for :reverse?).  A start key of nil (the
;; default) indicates first or last as appropriate for :reverse?.  If :keys-only? true, only
;; keys will be returned.  The default is false, in which case, map-entries (vector of key
;; and value) are returned as appropriate for reduce-kv.


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
     ;; faster if snapshot is sorted and bucket starts empty
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

;; same idea as contains? but implemented with a PKeyed protocol, using my perferred name
(defn key? [bucket key]
  (-key? bucket key))

(defn next-key [bucket key] (-next-key bucket key))

(defn previous-key [bucket key] (-previous-key bucket key))

(defn first-key [bucket] (-first-key bucket))

(defn last-key [bucket] (-last-key bucket))

;; returns plain hash-map (not sorted)
;; see also (persistent! bucket) which returns a sorted-map for better consistency with Bucket
(defn snapshot [bucket]
  (persistent! (reduce-kv assoc! (transient {}) bucket)))

