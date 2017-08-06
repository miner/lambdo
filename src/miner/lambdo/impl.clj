(ns miner.lambdo.impl
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (java.nio.charset StandardCharsets)
           (java.nio ByteBuffer)
           (clojure.lang MapEntry)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags Stat GetOp
                         ByteBufferProxy ByteArrayProxy Env$Builder
                         KeyRange
                         Cursor CursorIterator CursorIterator$KeyVal) ))

;; All of impl is essentially private, not part of the public API

;; SEM: Nippy encoding/decoding byte arrays into for LMDBjava to use.
;; SEM: use Edn encoding for keys so that we maintain LMDB lexigraphical order

;; Fressian was too hard to use, but maybe worth reconsidering.


;; For now, we want to be able to use either proxy.  Long term, we should choose just one
;; and hard-wire it.  Timing is currently about the same for my tests.  Theoretically,
;; PROXY_OPTIMAL should be faster, but maybe my encoding with nippy is slowing us down.
;; Note, we adjust to the proxy in the function lmdb-access

(def lmdb-proxy ByteBufferProxy/PROXY_OPTIMAL)
#_ (def lmdb-proxy ByteArrayProxy/PROXY_BA)

;; for debugging
(defn sysid [x]
  (format "%s-%H" (.getSimpleName ^Class (class x)) (System/identityHashCode x) ))



(defn dbiflags ^"[Lorg.lmdbjava.DbiFlags;" [flags]
  (into-array DbiFlags flags))
                             
(defn txnflags ^"[Lorg.lmdbjava.TxnFlags;" [flags]
  (into-array TxnFlags flags))

(defn putflags ^"[Lorg.lmdbjava.PutFlags;" [flags]
  (into-array PutFlags flags))

(defn create-env
  (^Env [path] (create-env path 10))
  (^Env [path size-mb] (create-env path size-mb nil))
  (^Env [path size-mb flags]
   (let [^Env$Builder builder (-> (Env/create lmdb-proxy)
                                  (.setMapSize (* size-mb 1024 1024))
                                  (.setMaxDbs 16))]
     ^Env (.open builder (io/file path) (into-array EnvFlags flags)))))
  
(defn env-close [^Env env]
  (.close env))


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


(defn str-encode ^bytes [str]
  (when str
    (.getBytes ^String str StandardCharsets/UTF_8)))

(defn str-decode [^bytes barr]
  (when barr
    (String. barr StandardCharsets/UTF_8)))




(defn pr-encode ^bytes [val]
  (when val
    (.getBytes (pr-str val) StandardCharsets/UTF_8)))

(defn pr-decode [^bytes barr]
  (when barr
    (edn/read-string (String. barr StandardCharsets/UTF_8))))

(defn nip-encode ^bytes [val]
  (nip/fast-freeze val))

(defn nip-decode [barr]
  (when barr
    (nip/fast-thaw ^bytes barr)))




(defn pr-encode-byte-buffer ^ByteBuffer [val]
  (when val
    (let [raw ^bytes (.getBytes (pr-str val) StandardCharsets/UTF_8)
          ^ByteBuffer bb (ByteBuffer/allocateDirect (int (alength raw)))]
      (.flip (.put bb raw)))))

(defn pr-decode-byte-buffer [^ByteBuffer byte-buf]
  (when byte-buf
    (let [len (.limit byte-buf)
          barr (byte-array len)]
      (.rewind byte-buf)
      (.get byte-buf barr)
      (edn/read-string (String. barr StandardCharsets/UTF_8)))))

(defn nippy-encode-byte-buffer ^ByteBuffer [val]
  (when val
    (let [raw ^bytes (nip/fast-freeze val)
          ^ByteBuffer bb (ByteBuffer/allocateDirect (int (alength raw)))]
      (.flip (.put bb raw)))))

(defn nippy-decode-byte-buffer [^ByteBuffer byte-buf]
  (when byte-buf
    (let [len (.limit byte-buf)
          barr (byte-array len)]
      (.rewind byte-buf)
      (.get byte-buf barr)
      (nip/fast-thaw barr))))

;; SEM FIXME -- in theory, Dbi.reserve can give us the ByteBuffer we need and save doing a
;; copy later, but it needs txn, etc.  so must be done within Bucket



;; SEM: FIXME.  There must be a better way.  Maybe lower level.  But for now, we need
;; something that works.
;;
;; Explicitly check that the dbi has a key, as opposed to getting a nil val which could be a
;; miss or could be an actual nil value for that key.
(defn cursor-has-kcode? [^Cursor cursor kcode]
  (.get cursor kcode GetOp/MDB_SET))


;; Maybe improvements? Untested
(defn cursor-first-kcode [^Cursor cursor]
  (and (.first cursor) (.key cursor)))

(defn cursor-last-kcode [^Cursor cursor]
  (and (.last cursor) (.key cursor)))

(defn cursor-next-kcode [^Cursor cursor kcode]
  (if kcode
      (when (if (.get cursor kcode GetOp/MDB_SET)
              (.next cursor)
              (.get cursor kcode GetOp/MDB_SET_RANGE))
        (.key cursor))
    (cursor-first-kcode cursor)))
    
(defn cursor-previous-kcode [^Cursor cursor kcode]
  (if kcode
    (do
      (when-not (.get cursor kcode GetOp/MDB_SET)
        (.get cursor kcode GetOp/MDB_SET_RANGE))
      (when (.prev cursor)
        (.key cursor)))
    (cursor-last-kcode cursor)))


;; start and end are inclusive,  "closed" intervals
;; must be encoded already (except nil)
;; start or end = nil means the first or last
;; step zero is undefined 
;; public miner.lambdo/reducible converts step nil and 0 to 1, so not handled here

;; SEM new requirement.  All keys must be encoded before calling!
(defn ^KeyRange key-range [start end step]
    (if (neg? step)
      (cond (and (nil? start) (nil? end)) (KeyRange/allBackward)
            (nil? start) (KeyRange/atMostBackward end)
            (nil? end) (KeyRange/atLeastBackward start)
            :else (KeyRange/closedBackward start end))
      (cond (and (nil? start) (nil? end))  (KeyRange/all)
            (nil? start)  (KeyRange/atMost end)
            (nil? end) (KeyRange/atLeast start)
            :else (KeyRange/closed start end))))

(defn abs ^long [^long n]
  (if (neg? n) (- n) n))



;; https://dev.clojure.org/jira/browse/CLJ-1708
;; problem with set! on mutable field when compiled into thunk (for non-terminal try)

(defn with-bucket-cursor* [bucket cursor-fn]
  (if-let [txn (-txn (-database bucket))]
    (let [cursor (.openCursor ^Dbi (-dbi bucket) txn)]
      (try (cursor-fn cursor)
           (finally (.close cursor))))
    (let [txn ^Txn (-rotxn (-database bucket))]
      (try (.renew txn)
           (let [cursor (if-let [ro ^Cursor (-ro-cursor bucket)]
                          (doto ro (.renew txn))
                          (-set-ro-cursor! bucket (.openCursor ^Dbi (-dbi bucket) txn)))]
             (cursor-fn cursor))
           (finally (.reset txn))))))

(defmacro with-cursor [bucket cursor & body]
  `(with-bucket-cursor* ~bucket (fn [^Cursor ~cursor] ~@body)))


(defn  with-bucket-txn* [bucket txn-fn]
  (if-let [txn (-txn (-database bucket))]
    (txn-fn txn)
     (let [txn ^Txn (-rotxn (-database bucket))]
       (try (.renew txn)
            (txn-fn txn)
            (finally (.reset txn))))))


(defmacro with-txn [bucket txn & body]
  `(with-bucket-txn* ~bucket (fn [~txn] ~@body)))

;; bucket-reduce* now requires a real bucket, not just PBucketAccess

(defn bucket-reduce-kv [bucket f3 init start end step]
  (with-txn bucket txn
    (let [^Dbi dbi (-dbi bucket)
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range (-encode-key bucket start)
                                                            (-encode-key bucket end)
                                                            step))
          skip (long (dec (abs step)))]
      (loop [res init sk 0]
        (if (.hasNext iter)
          (let [^CursorIterator$KeyVal kv (.next iter)]
            ;; always call .next to advance iter
            (if (pos? sk)
              (recur res (dec sk))
              (let [k (-decode-key bucket ^bytes (.key kv))
                    v (-decode-val bucket ^bytes (.val kv))
                    res (f3 res k v)]
                (if (reduced? res)
                  @res
                  (recur res skip)))))
          res)))))

(defn bucket-reduce [bucket f init start end step]
  (with-txn bucket txn
    (let [^Dbi dbi (-dbi bucket)
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range (-encode-key bucket start)
                                                            (-encode-key bucket end)
                                                            step))
          skip (long (dec (abs step)))]
      (loop [res init sk 0]
        (if (.hasNext iter)
          (let [^CursorIterator$KeyVal kv (.next iter)]
            ;; always call .next to advance iter
            (if (pos? sk)
              (recur res (dec sk))
              (let [k (-decode-key bucket ^bytes (.key kv))
                    v (-decode-val bucket ^bytes (.val kv))
                    res (f res (MapEntry/create k v))]
                (if (reduced? res)
                  @res
                  (recur res skip)))))
          (f res))))))

(defn bucket-reduce-keys [bucket f init start end step]
  (with-txn bucket txn
    (let [^Dbi dbi (-dbi bucket)
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range (-encode-key bucket start)
                                                            (-encode-key bucket end)
                                                            step))
          skip (long (dec (abs step)))]
      (loop [res init sk 0]
        (if (.hasNext iter)
          (let [^CursorIterator$KeyVal kv (.next iter)]
            ;; always call .next to advance iter
            (if (pos? sk)
              (recur res (dec sk))
              (let [k (-decode-key bucket ^bytes (.key kv))
                    res (f res k)]
                (if (reduced? res)
                  @res
                  (recur res skip)))))
          res)))))




;; pr-compare needs to agree with order implied by LMDB lexigraphical order of results of
;; key-encode, which uses pr-str
(defn pr-compare [a b]
  (compare (pr-str a) (pr-str b)))


;; default is false for everything else
(extend-protocol PSortedSnapshot
  clojure.lang.PersistentTreeMap
  (sorted-snapshot? [this] (= (.comparator ^clojure.lang.PersistentTreeMap this) pr-compare)))


;; SEM FIXME: might be leaking a ro-cursor
;; never close the dbi, because LMDB does it that way

;; SEM Refactoring:  encoder is really PBucketAccess

;; Be careful about changing field names, some macros literally depend on them.
(deftype Bucket [database encoder ^:unsynchronized-mutable ^Cursor ro-cursor]
  
  PBucketExtra
  (-database [this] database)
  (-set-ro-cursor! [this cursor] (set! ro-cursor cursor))
  (-ro-cursor [this] ro-cursor)

  PBucketAccess
  (-dbi [this] (-dbi encoder))
  (-encode-key [this key] (-encode-key encoder key))
  (-decode-key [this raw] (-decode-key encoder raw))
  (-encode-val [this value] (-encode-val encoder val))
  (-decode-val [this raw] (-decode-val encoder raw))
  
  clojure.lang.ILookup
  (valAt [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-val encoder (with-txn this txn (.get ^Dbi (-dbi encoder) txn kcode)))))

  (valAt [this key not-found]
    (let [kcode (-encode-key encoder key)]
      (if-let [raw (with-cursor this cursor 
                     (when (cursor-has-kcode? cursor kcode)
                       (.val ^Cursor cursor)))]
        (-decode-val encoder ^bytes raw)
        not-found)))

  ;; No, it should not be Associative -- that implies IPersistentCollection and we are not
  ;; Persistent.  We would have to use separate transactions to allow the old to persist

  clojure.lang.ITransientMap
  (assoc [this key val]
    (io!)
    (if-let [tx (-txn database)]
      (let [kcode (-encode-key encoder key)]
        (.put ^Dbi (-dbi encoder) ^Txn tx kcode
              (-reserve-val encoder tx kcode val)
              (putflags [])))
      (.put ^Dbi (-dbi encoder) (-encode-key encoder key) (-encode-val encoder val)))
    this)

  (conj [this map-entry]
    (.assoc this (key map-entry) (val map-entry)))


  ;; SEM: FUTURE ISSUE -- uses pr-compare but we might specialize buckets later.
  ;; 
  ;; SEM: Issue -- this is expensive O(n), which is not the contract of persistent!
  ;; Also, the bucket is still usable after this call to persistent! since it makes a new
  ;; sorted-map.
  ;; SEM: maybe we should cache the persistent sorted-map.  Clear cache on any assoc!
  (persistent [this]
    (when-let [tx (-txn database)]
      (throw (ex-info "An open write transaction prevents persistent!.  The database must commit! or rollback! first."
                      {:txn tx
                       :database database
                       :bucket this})))
    (reduce-kv assoc (sorted-map-by pr-compare) this))

  (without [this key]
    (io!)
    (if-let [tx (-txn database)]
      (.delete ^Dbi (-dbi encoder) ^Txn tx (-encode-key encoder key))
      (.delete ^Dbi (-dbi encoder) (-encode-key encoder key)))
    this)

  (count [this]
    (with-txn this txn  (.entries ^Stat (.stat ^Dbi (-dbi encoder) txn))))

  clojure.lang.Seqable
  (seq [this]
    (bucket-reduce this conj () nil nil -1))

  clojure.lang.IReduce
  (reduce [this f]
    (bucket-reduce this f (f) nil nil 1))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (bucket-reduce this f init nil nil 1))

  clojure.lang.IKVReduce
  (kvreduce [this f3 init]
    (bucket-reduce-kv this f3 init nil nil 1))

  ;; SEM -- note Clojure ascending is opposite sense of lambdo reverse?
  clojure.lang.Sorted
  (comparator [this] pr-compare)
  (entryKey [this entry] (key entry))
  (seq [this ascending] (bucket-reduce this conj () nil nil (if ascending -1 1)))
  (seqFrom [this key ascending]
    (if ascending
      (bucket-reduce this conj () nil key -1)
      (bucket-reduce this conj () key nil 1)))

  PKeyed
  (-key? [this key]
    (let [kcode (-encode-key encoder key)]
      (with-cursor this cursor
        (cursor-has-kcode? cursor kcode))))
  
  PKeyNavigation
  (-first-key [this]
    (-decode-key encoder (with-cursor this cursor (cursor-first-kcode cursor))))
  (-last-key [this]
    (-decode-key encoder (with-cursor this cursor (cursor-last-kcode cursor))))
  (-next-key [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-key encoder (with-cursor this cursor (cursor-next-kcode cursor kcode)))))
  (-previous-key [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-key encoder (with-cursor this cursor (cursor-previous-kcode cursor kcode)))))
  
  PReducibleBucket
  (-reducible [this keys-only? start end step]
    (if keys-only?
      (reify
        clojure.lang.Seqable
        (seq [_]
          (seq (bucket-reduce-keys this conj [] start end step)))

        clojure.lang.IReduceInit
        (reduce [_ f init]
          (bucket-reduce-keys this f init start end step)))

      (reify
        clojure.lang.Seqable
        (seq [_]
          (seq (bucket-reduce this conj [] start end step)))

        clojure.lang.IReduceInit
        (reduce [_ f init]
          (bucket-reduce this f init start end step))

        clojure.lang.IKVReduce
        (kvreduce [_ f3 init]
          (bucket-reduce-kv this f3 init start end step)))))
  
  PAppendableBucket
  ;; bucket must fresh and sequential writes must be in key order
  (-append! [this key val]
    (io!)
    (if-let [tx (-txn database)]
      (let [kcode (-encode-key this key)]
        (.put ^Dbi (-dbi encoder) ^Txn tx kcode
              (-reserve-val encoder tx kcode val)
              (putflags [PutFlags/MDB_APPEND])))
      (throw (ex-info "Must be in transaction to append!"
                      {:bucket this
                       :key key
                       :val val})))
    this)

  )


(deftype ByteArrayAccess [^Dbi dbi]
  PBucketAccess
  (-dbi ^Dbi [this] dbi)
  (-encode-key [this key] (pr-encode key))
  (-decode-key [this barr] (pr-decode barr))
  (-encode-val [this val] (nip-encode val))
  (-reserve-val [this txn kcode val] (nip-encode val))
  (-decode-val [this barr] (nip-decode barr)))
    

(deftype ByteBufferAccess [^Dbi dbi]
  PBucketAccess
  (-dbi ^Dbi [this] dbi)
  (-encode-key [this key] (pr-encode-byte-buffer key))

  (-reserve-val [this txn kcode val]
    (when val
      (let [raw ^bytes (nip/fast-freeze val)
            ^ByteBuffer bb (.reserve dbi txn kcode (alength raw) (putflags []))]
        (.flip (.put bb raw)))))
  
  (-decode-key [this barr] (pr-decode-byte-buffer barr))
  (-encode-val [this val] (nippy-encode-byte-buffer val))
  (-decode-val [this barr] (nippy-decode-byte-buffer barr)))
    

;; hack to allow experimentation -- eventually, pick one and run with it.
(defn lmdb-access [dbi]
  (condp = lmdb-proxy
    ByteBufferProxy/PROXY_OPTIMAL (->ByteBufferAccess dbi)
    ByteArrayProxy/PROXY_BA (->ByteArrayAccess dbi)))


  
(deftype Database [dirpath
                  ^Env env
                  ^Txn ^:unsynchronized-mutable txn
                  ^Txn ^:unsynchronized-mutable rotxn]
  PDatabase
  (-txn [this] txn)

  (-rotxn [this] rotxn)

  (-open-bucket! [this bkey flags]
    (io!)
    (let [dbi (.openDbi env (pr-encode bkey) (dbiflags flags))]
      (->Bucket this (lmdb-access dbi) nil)))

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

  (-bucket-keys [this]
    (map pr-decode (.getDbiNames env)))


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


