(ns miner.lambdo.protocols)


(defprotocol PKeyed
  (-has-key? [this key]))

(defprotocol PKeyNavigation
  (-first-key [this])
  (-last-key [this])
  (-next-key [this key])
  (-previous-key [this key]))

(defprotocol PDatabase
  (-db-reduce-keys [this f init start rev?])
  (-db-reduce-kv [this f3 init start rev?])
  (-db-transduce [this xform f init start rev?]))

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
