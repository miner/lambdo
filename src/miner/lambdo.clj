(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [miner.lambdo.util :refer :all]
            ;; [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags)
           (org.lmdbjava ByteArrayProxy Env$Builder)
           (java.nio ByteBuffer) ))


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
  ([path] (create-env path 10))
  ([path size-mb] (create-env path size-mb nil))
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



(defn nippy-encode ^bytes [val]
  (nip/freeze val))

(defn nippy-decode [^bytes barr]
  (nip/thaw barr))


(defn dbi-fetch [^Dbi dbi ^Txn txn key] 
  ;; takes a Clojure key and returns a Clojure value.
  (nippy-decode (.get dbi txn (nippy-encode key))))

;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

(defn dbi-store
  ([^Dbi dbi key val] (.put dbi (nippy-encode key) (nippy-encode val)))
  ([^Dbi dbi ^Txn txn key val] (dbi-store dbi txn key val nil))
  ([^Dbi dbi ^Txn txn key val flags]
   (.put dbi txn (nippy-encode key) (nippy-encode val) (into-array PutFlags flags))))




;; User API starts here   
;; LMDB resources encapsulated in single LDB record
;; Use Clojure transient update semantics.
;; That is, always capture the resulting object, rather than whacking in-place.

;; might want to save path too


(defn get-dbi [ldb binkey]
  (get-in ldb [:bins binkey]))


(defrecord LDB [dirpath ^Env env bins ^Txn txn txnflags ^Txn read-only-txn thread]
  java.io.Closeable
  (close [this]
    (when read-only-txn (.close read-only-txn))
    (when txn (.close txn))
    (when env (.close env))
    ;; do not close the Dbi handles, just nil
    (assoc this :env nil :txn nil :read-only-txn nil :thread nil :bins nil)))




(defn create-bin! [ldb bin-key]
  (let [env (:env ldb)
        dbi (.openDbi ^Env env (nippy-encode bin-key) (dbiflags [DbiFlags/MDB_CREATE]))]
    (update ldb :bins assoc bin-key dbi)))
    

(defn begin! [ldb]
  (let [txn (:txn ldb)
        env (:env ldb)]
    (assoc ldb :txn (.txn ^Env env ^Txn txn (txnflags [])))))
;; but do we have to close the read-only-txn first?
  

(defn commit! [ldb]
  (if-let [^Txn txn (:txn ldb)]
    (let [parent (.getParent txn)]
      (.commit txn)
      (assoc ldb :txn parent))
    ldb))

(defn rollback! [ldb]
  (if-let [^Txn txn (:txn ldb)]
    (let [parent (.getParent txn)]
      (.abort txn)
      (assoc ldb :txn parent))
    ldb))

(comment ;; old code
    (cond (nil? txn) this
          (.isReadOnly txn) (do (doto txn (.reset) (.renew)) this)
          :else (do (doto txn (.commit) (.close))
                    (assoc this :txn nil)))) 

(defn fetch [ldb binkey key]
  (if-let [dbi (get-dbi ldb binkey)]
    (if-let [txn (:txn ldb)]
      (dbi-fetch dbi txn key)
      (let [^Txn rotxn (doto ^Txn (:read-only-txn ldb) (.renew))
            result (dbi-fetch dbi rotxn key)]
        (.reset rotxn)
        result))
    (throw (ex-info (str "Missing DBI fetching " binkey key ", not in "
                         (sequence (keys (:bins ldb))) ".")
                    {:binkey binkey
                     :key key
                     :bin-keys (keys (:bins ldb))}))))


;; SEM FIXME, need cursor support
(defn fetch-seq [ldb bin start end]
  (list (fetch ldb bin start)
        (fetch ldb bin end)))
  
(defn store! [ldb binkey key val]
  (if-let [dbi (get-dbi ldb binkey)]
    (dbi-store dbi (:txn ldb) key val)
    (throw (ex-info (str "Missing DBI for " binkey ", not in " (sequence (keys (:bins ldb))) ".")
                    {:binkey binkey
                     :bin-keys (keys (:bins ldb))})))
  ldb)



  
;; could add & kvs arity


;; SEM FIXME: do something with options
(defn create-ldb [dirpath size-mb options]
  (let [^Env env (create-env dirpath (or size-mb 10))]
    (map->LDB {:dirpath dirpath
               :env env
               :read-only-txn (doto (.txnRead env) (.reset))
               :thread (.getId (Thread/currentThread))})))



;; SEM FIXME: do something with options
(defn open-ldb [dirpath options]
  (let [^Env env (create-env dirpath)
        dbinames (seq (.getDbiNames env))
        bins (zipmap (map nippy-decode dbinames)
                     (map #(.openDbi env ^bytes % (dbiflags [])) dbinames))
        rotxn (doto (.txnRead env) (.reset))]
    (map->LDB {:dirpath dirpath
               :env env
               :thread (.getId (Thread/currentThread))
               :read-only-txn rotxn
               :bins bins})))

(defn close-ldb! [^LDB ldb]
  (.close ldb)
  ldb)
