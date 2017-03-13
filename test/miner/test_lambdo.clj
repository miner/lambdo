(ns miner.test-lambdo
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [miner.lambdo :refer :all]))

;; just for hacking, normally you'll want to use a permanent dir
(defn make-tmpdir
  ([] (make-tmpdir "LAMBDO"))
  ([basename] (let [tmp (io/file (System/getenv "TMPDIR") basename ".")]
                (io/make-parents tmp)
                tmp)))


(deftest a-test
  (testing "Make an LMDB environment"
    (is (create-env (make-tmpdir "LAMBDO_TEST")))))
