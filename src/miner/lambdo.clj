(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nip])
  (:import (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags)
           (java.nio ByteBuffer)
           java.nio.charset.StandardCharsets ))

;; SEM: Nippy encoding/decoding byte arrays into ByteBuffer for LMDBjava to use.
;; Fressian was too hard to use.

(defn str->bytes [s]
  (when s
    (.getBytes ^String s StandardCharsets/UTF_8)))

(defn bytes->str [bs]
  (when bs
    (String. ^bytes bs StandardCharsets/UTF_8)))


(defn create-env
  ([path] (create-env path 10))
  ([path size-mb] (create-env path size-mb nil))
  ([path size-mb flags]  (Env/open (io/file path) size-mb (into-array EnvFlags flags))))


(defn env-close [^Env env]
  (.close env))

(defn db-names [^Env env]
  (map bytes->str (.getDbiNames env)))


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



(defn encode ^ByteBuffer [val]
  (let [raw ^bytes (nip/freeze val)
        ^ByteBuffer bb (ByteBuffer/allocateDirect (int (alength raw)))]
    (.flip (.put bb raw))))

(defn decode [^ByteBuffer byte-buf]
  (let [len (.limit byte-buf)
        barr (byte-array len)]
    (.rewind byte-buf)
    (.get byte-buf barr)
    (nip/thaw barr)))




(defn get-val
  ;; takes a Clojure key and returns a Clojure value.
  ([^Dbi db ^Txn txn key] (decode (.get db txn (encode key)))))


  ;; Not sure about making the temp txn and committing it for a read.  Maybe should reset???
  #_ ([^Dbi db key]
   (with-open [txn (read-txn (.-env db))]
     (let [val (get-val db txn key)]
       (txn-reset txn)
       val)))



(defn put-val
  ([^Dbi db key val] (.put db (encode key) (encode val)))
  ([^Dbi db ^Txn txn key val flags]
   (.put db txn (encode key) (encode val) (into-array PutFlags flags))))



  





   
(comment


  )
