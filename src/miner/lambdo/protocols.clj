(ns miner.lambdo.protocols)


(defprotocol PKeyed
  (-has-key? [this key]))

(defprotocol PKeyNavigation
  (-first-key [this])
  (-last-key [this])
  (-next-key [this key])
  (-previous-key [this key]))

(defprotocol PReducibleDatabase
  (-reducible-keys [this start rev?])
  (-reducible-kvs [this start rev?]))

(defprotocol PStorage
  (-open-database! [this dbkey flags])
  (-begin! [this flags])
  (-commit! [this])
  (-rollback! [this])
  (-txn [this])
  (-rotxn [this]))

  
(extend-protocol PKeyed
  clojure.lang.Associative
  (-has-key? [this key] (.containsKey this key))
  clojure.lang.IPersistentSet
  (-has-key? [this key] (.contains this key))
  )
