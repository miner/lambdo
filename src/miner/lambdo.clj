(ns miner.lambdo
  (:require [clojure.java.io :as io]
            [clojure.data.fressian :as fr]
            #_ [taoensso.nippy :as nip])
  (:import (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags)
           (java.nio ByteBuffer)
           java.nio.charset.StandardCharsets ))

;; SEM: Considering nippy for future use.  For now, concentrating on using Fressian for data
;; encoding.


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

(defn db-open
  ;; returns Dbi
  ([env] (db-open env nil))
  ([env dbname] (db-open env dbname [DbiFlags/MDB_CREATE]))
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

;; Need to verify  Fressian usage.  Are the ByteBuffers supported directly or do we have to
;; call .array to get byte arrays for Fressian?  For read-object???

(defn encode ^ByteBuffer [val]
  (fr/write-object key))

(defn decode [^ByteBuffer byte-buf]
  (fr/read-object byte-buf))




(defn get-val
  ;; takes a Clojure key and returns a Clojure value.
  ;; Uses Fressian encoding internally.
  ;; Not sure about making the temp txn and committing it for a read.  Maybe should reset???
  ([^Dbi db key]
   (with-open [txn (read-txn (.-env db))]
     (let [val (get-val db txn key)]
       (txn-reset txn)
       val)))
  ([^Dbi db ^Txn txn key] (decode (.get db txn (encode key)))))

(defn put-val
  ([^Dbi db key val] (.put db (encode key) (encode val)))
  ([^Dbi db ^Txn txn key val flags]
   (.put db txn (encode key) (encode val) (into-array PutFlags flags))))



  





   
