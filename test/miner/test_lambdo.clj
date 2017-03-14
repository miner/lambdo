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
      (store db "foo" "bar")
      (store db 1 "one")
      (store db "one" 1)
      (store db '[ka] '[k a])
      (store db :a1 '{:a1 "a1" :A1 '(a 1)})
      (let [txn (read-txn env)
            foo (fetch db txn "foo")
            x1 (fetch db txn 1)
            one (fetch db txn "one")
            ka (fetch db txn '[ka])
            a1 (fetch db txn :a1)]
        (txn-reset txn)
        (is (= foo "bar"))
        (is (= x1 "one"))
        (is (= one 1))
        (is (= ka '[k a]))
        (is (= a1 '{:a1 "a1" :A1 '(a 1)}))
        ))))



