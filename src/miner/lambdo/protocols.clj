(ns miner.lambdo.protocols
  (:import (org.lmdbjava KeyRange)))

(defprotocol PBucket
  (-database [this])
  (-dbi [this])
  ;; https://dev.clojure.org/jira/browse/CLJ-1023  work-around
  ;; hack to make mutable ro-cursor accessible
  (-set-ro-cursor! [this cursor])
  (-ro-cursor [this])
  (-encode-key [this key])
  (-decode-key [this raw])
  (-encode-val [this value])
  (-decode-val [this raw])
  (-key-range ^KeyRange [this start end step])
  ;; SEM dubious benefit, more complexity, probably not worth it
  (-reserve-val [this txn kcode value]))


;; open way to implement clojure.core/contains?, but with a better name
(defprotocol PKeyed
  (-key? [this key]))

(defprotocol PDatabase
  (-env ^Env [this])
  (-txn ^Txn [this])
  (-maxKeySize [this])
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

