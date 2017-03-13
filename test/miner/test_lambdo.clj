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


(deftest basic-test
  (testing "Make an LMDB environment"
    (let [env (create-env (make-tmpdir "LAMBDO_TEST"))
          db (open-db env)
          ]
      (is env)
      (is db)
      (put-val db "foo" "bar")
      (put-val db 1 "one")
      (put-val db "one" 1)
      (let [txn (read-txn env)
            foo (get-val db txn "foo")
            x1 (get-val db txn 1)
            one (get-val db txn "one")]
        (txn-reset txn)
        (is (= foo "bar"))
        (is (= x1 "one"))
        (is (= one 1))))))

