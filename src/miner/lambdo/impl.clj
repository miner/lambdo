(ns miner.lambdo.impl
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            ;; [clojure.data.fressian :as fress]
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
;; Note, we adjust to the proxy in the function lmdb-bucket-constructor but this is just for
;; experimentation.


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


#_
(defn str-encode ^bytes [str]
  (when str
    (.getBytes ^String str StandardCharsets/UTF_8)))

#_
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



;; SEM FIXME: Must copy to get into direct BB, for now.
#_
(defn fressian-encode ^ByteBuffer [val]
  (when val
  (let [^ByteBuffer heap-bb (fress/write val)
        bb (ByteBuffer/allocateDirect (.capacity heap-bb))]
    (.flip (.put bb heap-bb)))))

;; using my snapshot of Fressian with :direct support
#_
(defn fressian-encode ^ByteBuffer [val]
  (when val
    (fress/write val :direct? true)))
#_
(defn fressian-decode [^ByteBuffer byte-buf]
  (when byte-buf
    (fress/read byte-buf)))


;;; SEM: some of these .rewind calls maybe should just be .flip
;;; not sure what the lmdbjava did when writing buffer.  Maybe don't need any move before
;;; reading?  Or should use direct (.getXXX buffer 0)

;;; SEM:  really want to reuse ByteBuffers as much as possible.  Pick a good size, and save
;;; the key-buff and val-buff.  If anything is too big, allocate a temp.  Might work.  The
;;; KEY and VAL buffs become part of the Bucket and have to be passed into the lower level
;;; calls.
;;;
;;; Or the ByteBuffers could be in a pool.  Ask for a size, get something reasonable or make
;;; a new one and retain it.  That would let the pool be part of the overall implementation
;;; (Database or Env level, not just bucket).   But are you sure you will quickly use the
;;; buffer???

;;; key-size is (.getMaxKeySize (.-env db)) -- typically 511



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

(comment "NOT TESTED, NOT USED"
(defn int-encode-byte-buffer ^ByteBuffer [i]
  (when i
    (let [bb (ByteBuffer/allocateDirect Integer/BYTES)]
      (.flip (.putInt bb (int i))))))

(defn int-decode-byte-buffer [^ByteBuffer byte-buf]
  (when byte-buf
    (.rewind byte-buf)
    (.getInt byte-buf)))


(defn int2-encode-byte-buffer ^ByteBuffer [i j]
  (when (and i j)
    (let [bb (ByteBuffer/allocateDirect (* 2 Integer/BYTES))]
      (-> bb
          (.putInt (int i))
          (.putInt (int j))
          .flip))))

(defn int2-decode-byte-buffer [^ByteBuffer byte-buf]
  (when byte-buf
    (.rewind byte-buf)
    (let [i (.getInt byte-buf)
          j (.getInt byte-buf)]
      [i j])))


(defn long-encode-byte-buffer ^ByteBuffer [i]
  (when i
    (let [bb (ByteBuffer/allocateDirect Long/BYTES)]
      (.flip (.putLong bb (long i))))))

(defn long-decode-byte-buffer [^ByteBuffer byte-buf]
  (when byte-buf
    (.rewind byte-buf)
    (.getLong byte-buf)))
)



          


;; SEM FIXME -- in theory, Dbi.reserve can give us the ByteBuffer we need and save doing a
;; copy later, but it needs txn, etc.  so must be done within Bucket



;; SEM: FIXME.  There must be a better way.  Maybe lower level.  But for now, we need
;; something that works.
;;
;; Explicitly check that the dbi has a key, as opposed to getting a nil val which could be a
;; miss or could be an actual nil value for that key.
(defn cursor-has-kcode? [^Cursor cursor kcode]
  (.get cursor kcode GetOp/MDB_SET))

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



;; SEM FIXME:  need to recycle ByteBuffers for key encoding.  Could be in a macro.
#_
(with-encoded-keys [start (buffer-expr x y)
                    end (buffer-expr z)]
  body
  )

;;; But really only two BB ever needed for multiple keys



;; start and end are inclusive,  "closed" intervals
;; start or end = nil means the first or last
;; step zero is undefined 
;; public miner.lambdo/reducible converts step nil and 0 to 1, so not handled here

(defn key-range ^KeyRange [bucket start-key end-key step]
  (let [start (-encode-key bucket start-key)
        end (-encode-key bucket end-key)]
    (if (neg? step)
      (cond (and (nil? start) (nil? end)) (KeyRange/allBackward)
            (nil? start) (KeyRange/atMostBackward end)
            (nil? end) (KeyRange/atLeastBackward start)
            :else (KeyRange/closedBackward start end))
      (cond (and (nil? start) (nil? end))  (KeyRange/all)
            (nil? start)  (KeyRange/atMost end)
            (nil? end) (KeyRange/atLeast start)
            :else (KeyRange/closed start end)))))




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
  `(with-bucket-cursor* ~bucket (fn [~cursor] ~@body)))


(defn  with-bucket-txn* [bucket txn-fn]
  (if-let [txn (-txn (-database bucket))]
    (txn-fn txn)
     (let [txn ^Txn (-rotxn (-database bucket))]
       (try (.renew txn)
            (txn-fn txn)
            (finally (.reset txn))))))


(defmacro with-txn [bucket txn & body]
  `(with-bucket-txn* ~bucket (fn [~txn] ~@body)))

(defn bucket-reduce-kv [bucket f3 init start end step]
  (with-txn bucket txn
    (let [^Dbi dbi (-dbi bucket)
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range bucket start end step))
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
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range bucket start end step))
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
          ^CursorIterator iter (.iterate dbi ^Txn txn (key-range bucket start end step))
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

;; Not sure about clojure.lang.Sequential -- in theory, it means anything that has a stable
;; order for iteration.  Buckets are sorted by key so that makes them sequential, I think.
;; But, clojure.lang.PersistentTreeMap (sorted-map ...) does not actually implement
;; clojure.lang.Sequential so one of us is mistaken.  ???  The alternative definition is
;; that c.l.Sequential is a collection that is not clojure.lang.Associative (not a map or set).

(defn -reducible [bucket keys-only? start end step]
  (if keys-only?
    (reify
      clojure.lang.Seqable
      (seq [_]
        (seq (bucket-reduce-keys bucket conj [] start end step)))

      clojure.lang.IReduceInit
      (reduce [_ f init]
        (bucket-reduce-keys bucket f init start end step))

      clojure.lang.Sequential)

    (reify
      clojure.lang.Seqable
      (seq [_]
        (seq (bucket-reduce bucket conj [] start end step)))

      clojure.lang.IReduceInit
      (reduce [_ f init]
        (bucket-reduce bucket f init start end step))

      clojure.lang.IKVReduce
      (kvreduce [_ f3 init]
        (bucket-reduce-kv bucket f3 init start end step))

      clojure.lang.Sequential) ))
  

;; This is only for fast-loading a fresh bucket.
;; The bucket must be fresh, and sequential writes must be in key order
(defn -append! [bucket key val]
  (io!)
  (if-let [txn (-txn (-database bucket))]
    (let [kcode (-encode-key bucket key)]
      (.put ^Dbi (-dbi bucket) ^Txn txn kcode
            (-reserve-val bucket txn kcode val)
            (putflags [PutFlags/MDB_APPEND])))
    (throw (ex-info "Must be in transaction to append!"
                    {:bucket bucket
                     :key key
                     :val val})))
  bucket)



(defn -valAt [bucket key not-found]
  (let [kcode (-encode-key bucket key)]
    (if (nil? not-found)
      (-decode-val bucket (with-txn bucket txn (.get ^Dbi (-dbi bucket) txn kcode))) 
      (if-let [raw (with-cursor bucket cursor 
                     (when (cursor-has-kcode? cursor kcode)
                       (.val ^Cursor cursor)))]
        (-decode-val bucket raw)
        not-found))))


(defn -assoc! [bucket key value]
  (io!)
  (let [kcode (-encode-key bucket key)
        dbi ^Dbi (-dbi bucket)]
    (if-let [txn (-txn (-database bucket))]
      (.put dbi ^Txn txn kcode
            (-reserve-val bucket txn kcode value)
            (putflags []))
      (.put dbi kcode (-encode-val bucket value))))
  bucket)


(defn -count [bucket]
  (with-txn bucket txn
    (.entries ^Stat (.stat ^Dbi (-dbi bucket) txn))))

(defn -bucket-key? [bucket key]
  (let [kcode (-encode-key bucket key)]
    (with-cursor bucket cursor
      (cursor-has-kcode? cursor kcode))))

(defn -without [bucket key]
  (io!)
  (if-let [txn (-txn (-database bucket))]
    (.delete ^Dbi (-dbi bucket) ^Txn txn (-encode-key bucket key))
    (.delete ^Dbi (-dbi bucket) (-encode-key bucket key)))
  bucket)


(deftype ByteBufferBucket [database ^Dbi dbi ^:unsynchronized-mutable ^Cursor ro-cursor]

  PBucket
  (-dbi ^Dbi [this] dbi)
  (-database [this] database)
  (-set-ro-cursor! [this cursor] (set! ro-cursor cursor))
  (-ro-cursor [this] ro-cursor)
  
  (-encode-key [this key] (pr-encode-byte-buffer key))

  (-reserve-val [this txn kcode val]
    (when val
      (let [raw ^bytes (nip/fast-freeze val)
            ^ByteBuffer bb (.reserve dbi txn kcode (alength raw) (putflags []))]
        (.flip (.put bb raw)))))
  
  (-decode-key [this barr] (pr-decode-byte-buffer barr))
  (-encode-val [this val] (nippy-encode-byte-buffer val))
  (-decode-val [this barr] (nippy-decode-byte-buffer barr))
  
  clojure.lang.ILookup
  (valAt [this key] (-valAt this key nil))
  (valAt [this key not-found] (-valAt this key not-found))

  ;; No, it should not be Associative -- that implies IPersistentCollection and we are not
  ;; Persistent.  We would have to use separate transactions to allow the old to persist

  clojure.lang.ITransientMap
  (assoc [this key value] (-assoc! this key value))
  (conj [this map-entry](-assoc! this (key map-entry) (val map-entry)))

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

  (without [this key] (-without this key))

  (count [this] (-count this))

  clojure.lang.Sequential
  clojure.lang.Seqable
  (seq [this] (bucket-reduce this conj () nil nil -1))

  clojure.lang.IReduce
  (reduce [this f] (bucket-reduce this f (f) nil nil 1))

  clojure.lang.IReduceInit
  (reduce [this f init] (bucket-reduce this f init nil nil 1))

  clojure.lang.IKVReduce
  (kvreduce [this f3 init] (bucket-reduce-kv this f3 init nil nil 1))

  clojure.lang.Sorted
  (comparator [this] pr-compare)
  (entryKey [this entry] (key entry))
  (seq [this ascending] (bucket-reduce this conj () nil nil (if ascending -1 1)))
  (seqFrom [this key ascending]
    (if ascending
      (bucket-reduce this conj () nil key -1)
      (bucket-reduce this conj () key nil 1)))

  PKeyed
  (-key? [this key] (-bucket-key? this key))

  )




(deftype ByteArrayBucket [database ^Dbi dbi ^:unsynchronized-mutable ^Cursor ro-cursor]

  PBucket
  (-dbi ^Dbi [this] dbi)
  (-database [this] database)
  (-set-ro-cursor! [this cursor] (set! ro-cursor cursor))
  (-ro-cursor [this] ro-cursor)
  (-encode-key [this key] (pr-encode key))
  (-decode-key [this barr] (pr-decode barr))
  (-encode-val [this val] (nip-encode val))
  (-reserve-val [this txn kcode val] (nip-encode val))
  (-decode-val [this barr] (nip-decode barr))
  
  clojure.lang.ILookup
  (valAt [this key] (-valAt this key nil))
  (valAt [this key not-found] (-valAt this key not-found))

  ;; No, it should not be Associative -- that implies IPersistentCollection and we are not
  ;; Persistent.  We would have to use separate transactions to allow the old to persist

  clojure.lang.ITransientMap
  (assoc [this key value] (-assoc! this key value))
  (conj [this map-entry](-assoc! this (key map-entry) (val map-entry)))

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

  (without [this key] (-without this key))

  (count [this] (-count this))

  clojure.lang.Sequential
  clojure.lang.Seqable
  (seq [this] (bucket-reduce this conj () nil nil -1))

  clojure.lang.IReduce
  (reduce [this f] (bucket-reduce this f (f) nil nil 1))

  clojure.lang.IReduceInit
  (reduce [this f init] (bucket-reduce this f init nil nil 1))

  clojure.lang.IKVReduce
  (kvreduce [this f3 init] (bucket-reduce-kv this f3 init nil nil 1))

  clojure.lang.Sorted
  (comparator [this] pr-compare)
  (entryKey [this entry] (key entry))
  (seq [this ascending] (bucket-reduce this conj () nil nil (if ascending -1 1)))
  (seqFrom [this key ascending]
    (if ascending
      (bucket-reduce this conj () nil key -1)
      (bucket-reduce this conj () key nil 1)))

  PKeyed
  (-key? [this key] (-bucket-key? this key))

  )




;; hack to allow experimentation -- eventually, pick one and run with it.
(defn lmdb-bucket-constructor [flags]
  ;; flags currently ignored
  (condp = lmdb-proxy
    ByteBufferProxy/PROXY_OPTIMAL ->ByteBufferBucket
    ByteArrayProxy/PROXY_BA ->ByteArrayBucket))
  

(defn -open-bucket! [db bkey flags]
  (io!)
  (let [dbi (.openDbi (-env db) (pr-encode bkey) (dbiflags flags))]
    ((lmdb-bucket-constructor flags) db dbi nil)))

(defn -begin! [db flags]
  (io!)
  (-set-txn! db (.txn (-env db) (-txn db) (txnflags flags)))
  db)

(defn -commit! [db]
  (when-let [txn (-txn db)]
    (io!)
    (let [parent (.getParent txn)]
      (.commit txn)
      (-set-txn! db parent)))
  db)

(defn -rollback! [db]
  (when-let [txn (-txn db)]
    (io!)
    (let [parent (.getParent txn)]
      (.abort txn)
      (-set-txn! db parent)))
    db)

(defn -bucket-keys [db]
  (map pr-decode (.getDbiNames (-env db))))


  
(deftype Database [dirpath
                  ^Env env
                  ^Txn ^:unsynchronized-mutable txn
                  ^Txn ^:unsynchronized-mutable rotxn]
  PDatabase
  (-txn ^Txn [this] txn)
  (-rotxn ^Txn [this] rotxn)
  (-set-txn! [this transaction] (set! txn transaction))
  (-env ^Env [this] env)

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


