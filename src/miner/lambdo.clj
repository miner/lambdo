(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [miner.lambdo.util :refer :all]
            [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (java.nio.charset StandardCharsets)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags
                         ByteArrayProxy Env$Builder
                         CursorIterator CursorIterator$KeyVal CursorIterator$IteratorType) ))



;; SEM: Nippy encoding/decoding byte arrays into for LMDBjava to use.
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


(defn DONT-db-close [env db]
  ;; normally don't need to call this
  ;; can usually just reuse the db handle
)

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

(defn val-decode [^bytes barr]
  (nip/fast-thaw barr))


(defn dbi-fetch [^Dbi dbi ^Txn txn key] 
  ;; takes a Clojure key and returns a Clojure value.
  (val-decode (.get dbi txn (key-encode key))))

;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

(defn dbi-store
  ([^Dbi dbi key val] (.put dbi (key-encode key) (val-encode val)))
  ([^Dbi dbi ^Txn txn key val] (dbi-store dbi txn key val nil))
  ([^Dbi dbi ^Txn txn key val flags]
   (.put dbi txn (key-encode key) (val-encode val) (into-array PutFlags flags))))


(defn dbi-reduce [^Dbi dbi ^Txn txn f3 init start-key rev?]
  (let [^CursorIterator iter (.iterate dbi txn (when start-key (key-encode start-key))
                                       (if rev?
                                         CursorIterator$IteratorType/BACKWARD
                                         CursorIterator$IteratorType/FORWARD))]
    (loop [res init]
      (if (.hasNext iter)
        (let [^CursorIterator$KeyVal kv (.next iter)
              k (key-decode ^bytes (.key kv))
              v (val-decode ^bytes (.val kv))
              res (f3 res k v)]
          (if (reduced? res)
            @res
            (recur res)))
        res))))

(defn dbi-keys [^Dbi dbi ^Txn txn start-key rev?]
  (let [^CursorIterator iter (.iterate dbi txn (when start-key (key-encode start-key))
                                       (if rev?
                                         CursorIterator$IteratorType/BACKWARD
                                         CursorIterator$IteratorType/FORWARD))]
    (loop [res []]
      (if (.hasNext iter)
        (let [^CursorIterator$KeyVal kv (.next iter)
              k (key-decode ^bytes (.key kv))]
          (recur (conj res k)))
        res))))



;; User API starts here   

(deftype Database [storage ^Dbi dbi]
  PDatabase
  (-fetch [this key]
    (if-let [txn (-txn storage)]
      (dbi-fetch dbi txn key)
      (let [^Txn rotxn (doto ^Txn (-rotxn storage) (.renew))
            result (dbi-fetch dbi rotxn key)]
        (.reset rotxn)
        result)))

  (-store! [this key val]
    (io!)
    (dbi-store dbi (-txn storage) key val)
    this)

  (-db-keys [this start rev?]
    (if-let [txn (-txn storage)]
      (dbi-keys dbi txn start rev?)
      (let [^Txn rotxn (doto ^Txn (-rotxn storage) (.renew))
            result (dbi-keys dbi rotxn start rev?)]
        (.reset rotxn)
        result)))

  (-db-reduce [this f3 init start rev?]
    (if-let [txn (-txn storage)]
      (dbi-reduce dbi txn f3 init start rev?)
      (let [^Txn rotxn (doto ^Txn (-rotxn storage) (.renew))
            result (dbi-reduce dbi rotxn f3 init start rev?)]
        (.reset rotxn)
        result)))
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
      (->Database this dbi)))

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



;; ldb is now typically a Storage
;; database is now typically a DBI





;; SEM FIXME, need cursor support
#_ (defn fetch-seq [ldb bin start end]
  (list (fetch ldb bin start)
        (fetch ldb bin end)))
  

  

;; SEM FIXME: do something with options,  create ignored

(defn open-storage ^Storage [dirpath & {:keys [size-mb create]}]
  (let [^Env env (create-env dirpath (or size-mb 10))]
    (->Storage dirpath
               env
               nil
               (doto (.txnRead env) (.reset)))))

(defn create-storage! ^Storage [dirpath & {:keys [size-mb]}]
  (open-storage dirpath :size-mb size-mb :create true))

(defn close-storage! [^Storage ldb]
  (io!)
  (.close ldb)
  ldb)

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

(defn keys-db
  ([db] (keys-db db nil))
  ([db start-key] (keys-db db start-key false))
  ([db start-key reverse?]
   (-db-keys db start-key reverse?)))


