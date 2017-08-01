(ns miner.lambdo.protocols)

(defprotocol PEncoder
  (-dbi [this])
  (-encode-key [this key])
  (-decode-key [this raw])
  (-encode-val [this value])
  (-decode-val [this raw]))


(defprotocol PBucket
  (-encoder [this]))

;; open way to implement clojure.core/contains?, but with a better name
(defprotocol PKeyed
  (-key? [this key]))

(defprotocol PKeyNavigation
  (-first-key [this])
  (-last-key [this])
  (-next-key [this key])
  (-previous-key [this key]))

(defprotocol PReducibleBucket
  (-reducible [this keys-only? start end step]))

(defprotocol PAppendableBucket
  (-append! [this key val]))

(defprotocol PDatabase
  (-open-bucket! [this bkey flags])
  (-begin! [this flags])
  (-commit! [this])
  (-rollback! [this])
  (-txn [this])
  (-rotxn [this])
  (-bucket-keys [this]))

(defprotocol PSortedSnapshot
  (sorted-snapshot? [this]))


(extend-protocol PKeyed
  clojure.lang.Associative
  (-key? [this key] (.containsKey this key))
  clojure.lang.IPersistentSet
  (-key? [this key] (.contains this key))
  )

(extend-protocol PSortedSnapshot
  Object
  (sorted-snapshot? [this] false))

