(ns miner.lambdo.impl
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (java.nio.charset StandardCharsets)
           (clojure.lang MapEntry)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags Stat GetOp
                         ByteArrayProxy Env$Builder
                         Cursor CursorIterator CursorIterator$KeyVal CursorIterator$IteratorType) ))



;; SEM: Nippy encoding/decoding byte arrays into for LMDBjava to use.
;; SEM: use Edn encoding for keys so that we maintain LMDB lexigraphical order

;; Fressian was too hard to use, but maybe worth reconsidering.
  
;; for debugging
(defn sysid [x]
  (format "%s-%H" (.getSimpleName ^Class (class x)) (System/identityHashCode x) ))



(defn dbiflags ^"[Lorg.lmdbjava.DbiFlags;" [flags]
  (into-array DbiFlags flags))
                             
(defn txnflags ^"[Lorg.lmdbjava.TxnFlags;" [flags]
  (into-array TxnFlags flags))

(defn create-env
  (^Env [path] (create-env path 10))
  (^Env [path size-mb] (create-env path size-mb nil))
  (^Env [path size-mb flags]
   (let [^Env$Builder builder (-> (Env/create ByteArrayProxy/PROXY_BA)
                                  (.setMapSize (* size-mb 1024 1024))
                                  (.setMaxDbs 16))]
     ^Env (.open builder (io/file path) (into-array EnvFlags flags)))))
  
(defn env-close [^Env env]
  (.close env))

;; Should be obsolete, but still used in tests
(defn open-dbi
  ;; returns Dbi
  ([env] (open-dbi env nil))
  ([env dbname] (open-dbi env dbname [DbiFlags/MDB_CREATE]))
  ([^Env env ^String dbname flags]   (.openDbi env
                                               dbname
                                               (dbiflags flags))))




;; could use with-open to create txn
;; but it's a feature that txn can be recycled with reset and renew so you don't always want
;; to close them



;; might be nested in parent, but can be nil
(defn create-txn [^Env env ^Txn parent flags]
  (.txn env parent (txnflags flags)))

(defn read-txn ^Txn [^Env env]
  (.txnRead env))

(defn write-txn ^Txn [^Env env]
  (.txnWrite env))


(defn txn-commit [^Txn txn]
  (.commit txn))

(defn txn-abort [^Txn txn]
  (.abort txn))

(defn txn-reset [^Txn txn]
  ;; reset read-only transaction so that it can be "renewed" without having to abort
  (.reset txn))

(defn txn-renew [^Txn txn]
  ;; reuse read-only txn that has been reset, otherwise error
  (.renew txn))

(defn txn-close [^Txn txn]
  (.close txn))

;; Note: we want to preserve lexigraphical sorting of keys so we can't use nippy encoding.
;; We're using edn strings instead.  UTF_8 should be good for mostly ASCII strings, and safe for
;; cross-platform use.

(defn key-encode ^bytes [val]
  (.getBytes (pr-str val) StandardCharsets/UTF_8))

(defn key-decode [^bytes barr]
  (edn/read-string (String. barr StandardCharsets/UTF_8)))

(defn val-encode ^bytes [val]
  (nip/fast-freeze val))

(defn val-decode [barr]
  (when barr
    (nip/fast-thaw ^bytes barr)))


(defn dbi-fetch [^Dbi dbi ^Txn txn key] 
  ;; takes a Clojure key and returns a Clojure value.
  (val-decode (.get dbi txn (key-encode key))))


;; SEM: FIXME.  There must be a better way.  Maybe lower level.  But for now, we need
;; something that works.
;;
;; Explicitly check that the dbi has a key, as opposed to getting a nil val which could be a
;; miss or could be an actual nil value for that key.
(defn cursor-has-key? [^Cursor cursor key]
  (let [kcode (key-encode key)]
    (.get cursor kcode GetOp/MDB_SET)))


;; Maybe improvements? Untested
(defn cursor-first-key [^Cursor cursor]
  (and (.first cursor) (key-decode (.key cursor))))

(defn cursor-last-key [^Cursor cursor]
  (and (.last cursor) (key-decode (.key cursor))))

(defn cursor-next-key [^Cursor cursor key]
  (if key
    (let [kcode (key-encode key)]
      (when (if (.get cursor kcode GetOp/MDB_SET)
              (.next cursor)
              (.get cursor kcode GetOp/MDB_SET_RANGE))
        (key-decode (.key cursor))))
    (cursor-first-key cursor)))
    
(defn cursor-previous-key [^Cursor cursor key]
  (if key
    (let [kcode (key-encode key)]
      (when-not (.get cursor kcode GetOp/MDB_SET)
        (.get cursor kcode GetOp/MDB_SET_RANGE))
      (when (.prev cursor)
        (key-decode (.key cursor))))
    (cursor-last-key cursor)))


(defn dbi-count [^Dbi dbi ^Txn txn]
  (.entries ^Stat (.stat dbi txn)))
    
  
;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

(defn dbi-store
  ([^Dbi dbi key val] (.put dbi (key-encode key) (val-encode val)))
  ([^Dbi dbi ^Txn txn key val] (dbi-store dbi txn key val nil))
  ([^Dbi dbi ^Txn txn key val flags]
   (.put dbi txn (key-encode key) (val-encode val) (into-array PutFlags flags))))


(defn dbi-delete
  ([^Dbi dbi key val] (.delete dbi (key-encode key)))
  ([^Dbi dbi ^Txn txn key val] (.delete dbi txn (key-encode key))))


(defn dbi-reduce-kv [^Dbi dbi ^Txn txn f3 init start-key rev?]
  (let [^CursorIterator iter (.iterate dbi txn (when start-key (key-encode start-key))
                                       (if rev?
                                         CursorIterator$IteratorType/BACKWARD
                                         CursorIterator$IteratorType/FORWARD))]
    (loop [res init check-first (and rev? start-key)]
      (if (.hasNext iter)
        (let [^CursorIterator$KeyVal kv (.next iter)
              k (key-decode ^bytes (.key kv))]
          (if (and check-first (not= k start-key))
            (recur res nil)
            (let [v (val-decode ^bytes (.val kv))
                  res (f3 res k v)]
              (if (reduced? res)
                @res
                (recur res nil)))))
        res))))

(defn dbi-reduce [^Dbi dbi ^Txn txn f init start-key rev?]
  (let [^CursorIterator iter (.iterate dbi txn (when start-key (key-encode start-key))
                                       (if rev?
                                         CursorIterator$IteratorType/BACKWARD
                                         CursorIterator$IteratorType/FORWARD))]
    (loop [res init check-first (and rev? start-key)]
      (if (.hasNext iter)
        (let [^CursorIterator$KeyVal kv (.next iter)
              k (key-decode ^bytes (.key kv))]
          (if (and check-first (not= k start-key))
            (recur res nil)
            (let [v (val-decode ^bytes (.val kv))
                  res (f res (MapEntry/create k v))]
              (if (reduced? res)
                @res
                (recur res nil)))))
        (f res)))))

(defn dbi-reduce-keys [^Dbi dbi ^Txn txn f init start-key rev?]
  (let [^CursorIterator iter (.iterate dbi txn (when start-key (key-encode start-key))
                                       (if rev?
                                         CursorIterator$IteratorType/BACKWARD
                                         CursorIterator$IteratorType/FORWARD))]
    (loop [res init check-first (and rev? start-key)]
      (if (.hasNext iter)
        (let [^CursorIterator$KeyVal kv (.next iter)
              k (key-decode ^bytes (.key kv))]
          (if (and check-first (not= k start-key))
            (recur res nil)
            (let [res (f res k)]
              (if (reduced? res)
                @res
                (recur res nil)))))
        res))))


;; may only be used within Database functions, assumes access to Database fields
(defmacro -Database-with-txn [txn & body]
  `(if-let [~txn (-txn ~'storage)]
     (do ~@body)
     (let [~txn ^Txn (-rotxn ~'storage)]
       (try (.renew ~txn)
            ~@body
            (finally (.reset ~txn))))))


(defmacro -Database-with-txn-cursor [txn cursor & body]
  `(if-let [~txn (-txn ~'storage)]
     (let [~cursor (.openCursor ~'dbi ~txn)]
       (try ~@body
            (finally (.close ~cursor))))
     (let [~txn ^Txn (-rotxn ~'storage)]
       (try (.renew ~txn)
            (if ~'ro-cursor
              (.renew ~'ro-cursor ~txn)
              (set! ~'ro-cursor (.openCursor ~'dbi ~txn)))
            (let [~cursor ~'ro-cursor]
              ~@body)
            (finally (.reset ~txn))))))


(deftype Database [storage ^Dbi dbi ^:volatile-mutable ^Cursor ro-cursor]
  clojure.lang.ILookup
  (valAt [this key] (-Database-with-txn txn (dbi-fetch dbi txn key)))
  (valAt [this key not-found]
    (-Database-with-txn-cursor txn cursor
                        (if (cursor-has-key? cursor key)
                          (val-decode ^bytes (.val ^Cursor cursor))
                          not-found)))

  ;; No, it should not be Associative -- that implies IPersistentCollection and we are not
  ;; Persistent.  We would have to use separate transactions to allow the old to persist

  clojure.lang.ITransientMap
  (assoc [this key val]
    (io!)
    (if-let [tx (-txn storage)]
      (dbi-store dbi tx key val)
      (dbi-store dbi key val))
    this)

  (conj [this map-entry]
    (.assoc this (key map-entry) (val map-entry)))

  ;; SEM FIXME: should this be a storage commit! ???, probably not
  ;; no-op
  (persistent [this] this)

  (without [this key]
    (io!)
    (if-let [tx (-txn storage)]
      (dbi-delete dbi tx key val)
      (dbi-delete dbi key val))
    this)

  (count [this]
    (-Database-with-txn txn (dbi-count dbi txn)))

  clojure.lang.IReduce
  (reduce [this f]
    (-Database-with-txn txn (dbi-reduce dbi txn f (f) nil false)))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (-Database-with-txn txn (dbi-reduce dbi txn f init nil false)))

  clojure.lang.IKVReduce
  (kvreduce [this f3 init]
    (-Database-with-txn txn (dbi-reduce-kv dbi txn f3 init nil false)))
  
  PKeyed
  (-has-key? [this key]
    (-Database-with-txn-cursor txn cursor
                               (cursor-has-key? cursor key)))
  PKeyNavigation
  (-first-key [this]
    (-Database-with-txn-cursor txn cursor
                               (cursor-first-key cursor)))
  (-last-key [this]
    (-Database-with-txn-cursor txn cursor
                               (cursor-last-key cursor)))
  (-next-key [this key]
    (-Database-with-txn-cursor txn cursor
                               (cursor-next-key cursor key)))
  (-previous-key [this key]
    (-Database-with-txn-cursor txn cursor
                               (cursor-previous-key cursor key)))
  
  PReducibleDatabase
  (-reducible-keys [this start rev?]
    (reify
      ;; clojure.lang.IReduce
      #_ (reduce [this f]
        (-Database-with-txn txn (dbi-reduce-keys dbi txn f (f) start rev?)))

      clojure.lang.IReduceInit
      (reduce [this f init]
        (-Database-with-txn txn (dbi-reduce-keys dbi txn f init start rev?)))
      ))
  

  (-reducible-kvs [this start rev?]
    (reify
     ;; clojure.lang.IReduce
      #_ (reduce [this f]
        (-Database-with-txn txn (dbi-reduce dbi txn f (f) start rev?)))

      clojure.lang.IReduceInit
      (reduce [this f init]
        (-Database-with-txn txn (dbi-reduce dbi txn f init start rev?)))

      clojure.lang.IKVReduce
      (kvreduce [this f3 init]
        (-Database-with-txn txn (dbi-reduce-kv dbi txn f3 init start rev?)))
      ))
  
  )


  
(deftype Storage [dirpath
                  ^Env env
                  ^Txn ^:unsynchronized-mutable txn
                  ^Txn ^:unsynchronized-mutable rotxn]
  PStorage
  (-txn [this] txn)

  (-rotxn [this] rotxn)

  (-open-database! [this dbkey flags]
    (io!)
    (let [dbi (.openDbi env (key-encode dbkey) (dbiflags flags))]
      (->Database this dbi nil)))

  (-begin! [this flags]
    (io!)
    (set! txn (.txn env ^Txn txn (txnflags flags)))
    this)

  (-commit! [this]
    (when txn
      (io!)
      (let [parent (.getParent txn)]
        (.commit txn)
        (set! txn parent)))
    this)

  (-rollback! [this]
    (when txn
      (io!)
      (let [parent (.getParent txn)]
        (.abort txn)
        (set! txn parent)))
    this)

  java.io.Closeable
  (close [this]
    (io!)
    (when rotxn
      (.close rotxn)
      (set! rotxn nil))
    (when txn
      (.close txn)
      (set! txn nil))
    (when env
      (.close env))
    ;; do not close the Dbi handles, just nil
    this))


