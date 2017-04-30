(ns miner.lambdo.protocols)


(defprotocol PDatabase
  (-fetch [this key])
  (-store! [this key val])
  (-db-reduce [this f3 init start rev?])
  (-db-transduce [this xform f init start rev?])
  (-db-keys [this start rev?]))

(defprotocol PStorage
  (-open-database! [this dbkey flags])
  (-begin! [this flags])
  (-commit! [this])
  (-rollback! [this])
  (-txn [this])
  (-rotxn [this]))


