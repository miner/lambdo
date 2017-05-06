(ns miner.lambdo.protocols)


(defprotocol PDatabase
  (-store! [this key val])
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


