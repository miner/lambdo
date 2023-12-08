(ns miner.lambdo.abs
  (:refer-clojure :exclude [abs]))

;; Do not load this unless it is actually required.  Clojure 1.11 defines `abs` so this file
;; is needed only for earlier versions.  In theory, it will not be compiled or loaded in
;; current runtimes.

;; Not sure if the primitive hints are worthwhile.

(defn abs ^long [^long n] (if (neg? n) (- n) n))

