(ns miner.lambdo.protocols)

;; PBucketExtra and PBucketAccess need refactoring
;; But PBucketAccess is used separately so be careful

(defprotocol PBucket
  (-database [this])
  ;; https://dev.clojure.org/jira/browse/CLJ-1023  work-around
  ;; hack to make mutable ro-cursor accessible
  (-set-ro-cursor! [this cursor])
  (-ro-cursor [this]))
  
(defprotocol PBucketAccess
  (-dbi [this])
  (-encode-key [this key])
  (-decode-key [this raw])
  (-encode-val [this value])
  (-reserve-val [this txn kcode value])
  (-decode-val [this raw]))

;; open way to implement clojure.core/contains?, but with a better name
(defprotocol PKeyed
  (-key? [this key]))

(defprotocol PDatabase
  (-env ^Env [this])
  (-txn ^Txn [this])
  (-rotxn ^Txn [this])
  (-set-txn! [this transaction]))

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

