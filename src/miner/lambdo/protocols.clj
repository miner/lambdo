(ns miner.lambdo.protocols)

;; open way to implement clojure.core/contains?, but with a better name
(defprotocol PKeyed
  (-key? [this key]))

(defprotocol PKeyNavigation
  (-first-key [this])
  (-last-key [this])
  (-next-key [this key])
  (-previous-key [this key]))

(defprotocol PReducibleBucket
  (-reducible [this keys-only? start-key reverse?]))

(defprotocol PAppendableBucket
  (-append! [this key val]))

(defprotocol PStorage
  (-open-bucket! [this bkey flags])
  (-begin! [this flags])
  (-commit! [this])
  (-rollback! [this])
  (-txn [this])
  (-rotxn [this]))

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

