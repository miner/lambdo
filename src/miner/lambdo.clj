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

;; obsolete
(defn db-names [^Env env]
  (map utf8->str (.getDbiNames env)))


;;; SEM -- might be better to make the flagMask ourselves if we had another constructor.
;;; Probably should refactor the lmdbjava Dbi class.

;; Obsolete
(defn open-dbi
  ;; returns Dbi
  ([env] (open-dbi env nil))
  ([env dbname] (open-dbi env dbname [DbiFlags/MDB_CREATE]))
  ([^Env env ^String dbname flags]   (.openDbi env
                                               dbname
                                               (dbiflags flags))))


(defn DONT-db-close [env db]
  ;; normally don't need to call this
  ;; can usually just reuse the db handle
)


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


(defn dbi-fetch
  ;; takes a Clojure key and returns a Clojure value.
  ([^Dbi db ^Txn txn key] (nippy-decode (.get db txn (nippy-encode key))))
  
  ;; Not sure about making the temp txn and txt-reset
  ;; SEM: FIXME private field access hack
  ([^Dbi db key]
   (with-open [txn (read-txn (private-field db "env"))]
     (let [val (dbi-fetch db txn key)]
       (txn-reset txn)
       val))))


;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

(defn dbi-store
  ([^Dbi db key val] (.put db (nippy-encode key) (nippy-encode val)))
  ([^Dbi db ^Txn txn key val flags]
   (.put db txn (nippy-encode key) (nippy-encode val) (into-array PutFlags flags))))




;; User API starts here   
;; LMDB resources encapsulated in single LDB record
;; Use Clojure immutable update semantics.
;; That is, always capture the resulting object, rather than whacking in-place.

;; might want to save path too


(defn get-dbi [ldb bin]
  (get (:dbs ldb) bin :missing))


(defrecord LDB [dirpath ^Env env dbs ^Txn txn txnflags ^Txn read-only-txn thread]
  java.io.Closeable
  (close [this]
    (when read-only-txn (.close read-only-txn))
    (when txn (.close txn))
    (when env (.close env))
    ;; do not close the Dbi handles, just nil
    (assoc this :env nil :txn nil :read-only-txn nil :thread nil :dbs nil)))




(defn create-bin [ldb bin-key]
  (let [dbs (:dbs ldb)
        env (:env ldb)
        dbi (.openDbi ^Env env (nippy-encode bin-key) (dbiflags [DbiFlags/MDB_CREATE]))]
    (update ldb :dbs (assoc bin-key dbi))))
    

(defn begin [ldb]
  (let [txn (:txn ldb)
        env (:evn ldb)]
    (assoc ldb :txn (.txn ^Env env ^Txn txn (txnflags [])))))
;; but do we have to close the read-only-txn first?
  

(defn commit [ldb]
  (if-let [^Txn txn (:txn ldb)]
    (let [parent (.getParent txn)]
      (.commit txn)
      (assoc ldb :txn parent))
    ldb))

(defn abort [ldb]
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

(defn fetch [ldb bin key]
  (dbi-fetch (:env ldb) (get-dbi ldb bin) key))
  
(defn fetch-seq [ldb bin start end]
  ;; SEM FIXME, need cursor support
  (list (fetch ldb bin start)
        (fetch ldb bin end)))
  
(defn store [ldb bin key val]
  (dbi-store (:env ldb) (get-dbi bin) key val))
;; could add & kvs arity


;; SEM FIXME: do something with options
(defn create-ldb [dirpath size-mb options]
  (let [ldb (map->LDB {:dirpath dirpath
                       :env (create-env dirpath (or size-mb 10))
                       :thread (.getId (Thread/currentThread))})]
    (assoc ldb :read-only-txn (.txnRead ^Env (:env ldb)))))


;; SEM FIXME: do something with options
(defn open-ldb [dirpath options]
  (let [^Env env (create-env dirpath)
        dbinames (seq (.getDbiNames env))]
    (map->LDB {:dirpath dirpath
               :env env
               :thread (.getId (Thread/currentThread))
               :dbs (zipmap (map nippy-decode dbinames)
                            (map #(.openDbi env ^bytes % (dbiflags [])) dbinames))})))

