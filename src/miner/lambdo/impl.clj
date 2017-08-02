(ns miner.lambdo.impl
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [miner.lambdo.protocols :refer :all]
            [taoensso.nippy :as nip])
  (:import (java.nio.charset StandardCharsets)
           (clojure.lang MapEntry)
           (org.lmdbjava Env EnvFlags Dbi DbiFlags Txn TxnFlags PutFlags Stat GetOp
                         ByteArrayProxy Env$Builder
                         KeyRange
                         Cursor CursorIterator CursorIterator$KeyVal) ))

;; All of impl is essentially private, not part of the public API

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

(defn putflags ^"[Lorg.lmdbjava.PutFlags;" [flags]
  (into-array PutFlags flags))



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

;; SEM REMOVE
;; Obsolete, but still used in tests, use Database -open-bucket! instead
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


#_ (defn dbi-fetch [^Dbi dbi ^Txn txn key] 
  ;; takes a Clojure key and returns a Clojure value.
  (val-decode (.get dbi txn (key-encode key))))


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


#_ (defn dbi-count [^Dbi dbi ^Txn txn]
  (.entries ^Stat (.stat dbi txn)))
    
  
;; return true if newly stored, false if key was already there and flags indicated not to
;; overwrite

;; SEM FIXME: do we really need multi-arity?  Probably not
#_ (defn dbi-store
  ([^Dbi dbi key val] (.put dbi (key-encode key) (val-encode val)))
  ([^Dbi dbi ^Txn txn key val] (dbi-store dbi txn key val nil))
  ([^Dbi dbi ^Txn txn key val flags]
   (.put dbi txn (key-encode key) (val-encode val) (putflags flags))))


#_ (defn dbi-delete
  ([^Dbi dbi key val] (.delete dbi (key-encode key)))
  ([^Dbi dbi ^Txn txn key val] (.delete dbi txn (key-encode key))))

;; start and end are inclusive
;; step zero is undefined (but treated like positive for now)

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


;; bucket-reduce* really just takes a PEncoder as first arg, usually a bucket, but not
;; necessarily

(defn bucket-reduce-kv  [bucket ^Txn txn f3 init start end step]
  (let [^Dbi dbi (-dbi bucket)
        ^CursorIterator iter (.iterate dbi txn (key-range (-encode-key bucket start)
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
        res))))

(defn bucket-reduce [bucket ^Txn txn f init start end step]
  (let [^Dbi dbi (-dbi bucket)
        ^CursorIterator iter (.iterate dbi txn (key-range (-encode-key bucket start)
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
        (f res)))))

(defn bucket-reduce-keys [bucket ^Txn txn f init start end step]
  (let [^Dbi dbi (-dbi bucket)
        ^CursorIterator iter (.iterate dbi txn (key-range (-encode-key bucket start)
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
        res))))


;; SEM: These macros should be ^:private

;; may only be used within Bucket functions, assumes access to Bucket fields
(defmacro  with-txn [txn & body]
  `(if-let [~txn (-txn ~'database)]
     (do ~@body)
     (let [~txn ^Txn (-rotxn ~'database)]
       (try (.renew ~txn)
            ~@body
            (finally (.reset ~txn))))))


;; https://dev.clojure.org/jira/browse/CLJ-1708
;; problem with set! on mutable field when compiled into thunk (for non-terminal try)

(defmacro  with-txn-cursor [txn cursor & body]
  `(if-let [~txn (-txn ~'database)]
     (let [~cursor (.openCursor ^Dbi (-dbi ~'encoder) ~txn)]
       (try ~@body
            (finally (.close ~cursor))))
     (let [~txn ^Txn (-rotxn ~'database)]
       (try (.renew ~txn)
            (let [~cursor (if ~'ro-cursor
                            (doto ~'ro-cursor (.renew ~txn))
                            (-set-ro-cursor! ~'this (.openCursor ^Dbi (-dbi ~'encoder) ~txn)))]
              ~@body)
            (finally (.reset ~txn))))))

(defmacro  with-cursor [cursor & body]
  `(with-txn-cursor txn# ~cursor ~@body))



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
(deftype Bucket [database encoder ^:volatile-mutable ^Cursor ro-cursor]

  PSetROCursor
  ;;  https://dev.clojure.org/jira/browse/CLJ-1023  work-around
  (-set-ro-cursor! [this cursor] (try (set! (.ro-cursor this) cursor)) cursor)
  
  PBucketAccess
  (-dbi [this] (-dbi encoder))
  (-encode-key [this key] (-encode-key encoder key))
  (-decode-key [this raw] (-decode-key encoder raw))
  (-encode-val [this value] (-encode-val encoder val))
  (-decode-val [this raw] (-decode-val encoder raw))
  
  clojure.lang.ILookup
  (valAt [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-val encoder (with-txn txn (.get ^Dbi (-dbi encoder) txn kcode)))))

  (valAt [this key not-found]
    (let [kcode (-encode-key encoder key)]
      (if-let [raw (with-cursor cursor 
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
      (.put ^Dbi (-dbi encoder) ^Txn tx (-encode-key encoder key) (-encode-val encoder val)
            (putflags []))
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
    (with-txn txn  (.entries ^Stat (.stat ^Dbi (-dbi encoder) txn))))

  clojure.lang.Seqable
  (seq [this]
    (with-txn txn (bucket-reduce this txn conj () nil nil -1)))

  clojure.lang.IReduce
  (reduce [this f]
    (with-txn txn (bucket-reduce this txn f (f) nil nil 1)))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (with-txn txn (bucket-reduce this txn f init nil nil 1)))

  clojure.lang.IKVReduce
  (kvreduce [this f3 init]
    (with-txn txn (bucket-reduce-kv this txn f3 init nil nil 1)))

  ;; SEM -- note Clojure ascending is opposite sense of lambdo reverse?
  clojure.lang.Sorted
  (comparator [this] pr-compare)
  (entryKey [this entry] (key entry))
  (seq [this ascending] (with-txn txn (bucket-reduce this txn conj () nil nil (if ascending -1 1))))
  (seqFrom [this key ascending]
    (with-txn txn
      (if ascending
        (bucket-reduce this txn conj () nil key -1)
        (bucket-reduce this txn conj () key nil 1)))) 

  PKeyed
  (-key? [this key]
    (let [kcode (-encode-key encoder key)]
      (with-cursor cursor
        (cursor-has-kcode? cursor kcode))))
  
  PKeyNavigation
  (-first-key [this]
    (-decode-key encoder (with-cursor cursor (cursor-first-kcode cursor))))
  (-last-key [this]
    (-decode-key encoder (with-cursor cursor (cursor-last-kcode cursor))))
  (-next-key [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-key encoder (with-cursor cursor (cursor-next-kcode cursor kcode)))))
  (-previous-key [this key]
    (let [kcode (-encode-key encoder key)]
      (-decode-key encoder (with-cursor cursor (cursor-previous-kcode cursor kcode)))))
  
  PReducibleBucket
  (-reducible [this keys-only? start end step]
    (if keys-only?
      (reify
        clojure.lang.Seqable
        (seq [this]
          (with-txn txn (bucket-reduce-keys encoder txn conj () end start (- step))))

        clojure.lang.IReduceInit
        (reduce [this f init]
          (with-txn txn (bucket-reduce-keys encoder txn f init start end step))))

      (reify
        clojure.lang.Seqable
        (seq [this]
          (with-txn txn (bucket-reduce encoder txn conj () end start (- step))))

        clojure.lang.IReduceInit
        (reduce [this f init]
          (with-txn txn (bucket-reduce encoder txn f init start end step)))

        clojure.lang.IKVReduce
        (kvreduce [this f3 init]
          (with-txn txn (bucket-reduce-kv encoder txn f3 init start end step))))))
  
  PAppendableBucket
  ;; bucket must fresh and sequential writes must be in key order
  (-append! [this key val]
    (io!)
    (if-let [tx (-txn database)]
      (.put ^Dbi (-dbi encoder) ^Txn tx (-encode-key encoder key) (-encode-val encoder val)
            (putflags [PutFlags/MDB_APPEND]))
      (throw (ex-info "Must be in transaction to append!"
                      {:bucket this
                       :key key
                       :val val})))
    this)

  )


(deftype GenericAccess [^Dbi dbi]
  PBucketAccess
  (-dbi ^Dbi [this] dbi)
  (-encode-key [this key] (pr-encode key))
  (-decode-key [this barr] (pr-decode barr))
  (-encode-val [this val] (nip-encode val))
  (-decode-val [this barr] (nip-decode barr)))
    

  
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
      (->Bucket this (GenericAccess. dbi) nil)))

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


