(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [miner.lambdo.util :refer :all]
            [taoensso.nippy :as nip])
  (:import (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags)
           (java.nio ByteBuffer) ))


;; SEM: Nippy encoding/decoding byte arrays into ByteBuffer for LMDBjava to use.
;; Fressian was too hard to use.

(defn create-env
  ([path] (create-env path 10))
  ([path size-mb] (create-env path size-mb nil))
  ([path size-mb flags]  (Env/open (io/file path) size-mb (into-array EnvFlags flags))))
  

(defn env-close [^Env env]
  (.close env))

(defn db-names [^Env env]
  (map utf8->str (.getDbiNames env)))


;;; SEM -- might be better to make the flagMask ourselves if we had another constructor.
;;; Probably should refactor the lmdbjava Dbi class.

(defn open-db
  ;; returns Dbi
  ([env] (open-db env nil))
  ([env dbname] (open-db env dbname [DbiFlags/MDB_CREATE]))
  ([^Env env ^String dbname flags]   (.openDbi env
                                               dbname
                                               ^"[Lorg.lmdbjava.DbiFlags;"
                                               (into-array DbiFlags flags))))


(defn DONT-db-close [env db]
  ;; normally don't need to call this
  ;; can usually just reuse the db handle
)


;; could use with-open to create txn
;; but it's a feature that txn can be recycled with reset and renew so you don't always want
;; to close them



;; might be nested in parent, but can be nil
(defn create-txn [^Env env ^Txn parent flags]
  (.txn env parent (into-array TxnFlags flags)))

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



;; In theory, all Clojure values and keys will be encoded/decoded so LMDB only sees byte
;; buffers.

;; Concerned about too many buffers.  Maybe should try to recycle with Lambdo???
;; Well, it looks like the internal proxy for the Env is going to manage these ByteBuffers
;; so it needs them.  Don't try to optimize yet.  Did a simpler version that used byte
;; arrays directly, but now I suspect that it will miss some optimizations for the
;; supposedly fastest "unsafe" proxy.  Note: I didn't performance test anything myself so I
;; might have guessed wrong.


(defn nippy-encode ^ByteBuffer [val]
  (let [raw ^bytes (nip/fast-freeze val)
        ^ByteBuffer bb (ByteBuffer/allocateDirect (int (alength raw)))]
    (.flip (.put bb raw))))

(defn nippy-decode [^ByteBuffer byte-buf]
  (let [len (.limit byte-buf)
        barr (byte-array len)]
    (.rewind byte-buf)
    (.get byte-buf barr)
    (nip/fast-thaw barr)))




(defn fetch
  ;; takes a Clojure key and returns a Clojure value.
  ([^Dbi db ^Txn txn key] (nippy-decode (.get db txn (nippy-encode key))))
  
  ;; Not sure about making the temp txn and txt-reset
  ;; SEM: FIXME private field access hack
  ([^Dbi db key]
   (with-open [txn (read-txn (private-field db "env"))]
     (let [val (fetch db txn key)]
       (txn-reset txn)
       val))))


;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

(defn store
  ([^Dbi db key val] (.put db (nippy-encode key) (nippy-encode val)))
  ([^Dbi db ^Txn txn key val flags]
   (.put db txn (nippy-encode key) (nippy-encode val) (into-array PutFlags flags))))




   




   
(comment


  )
