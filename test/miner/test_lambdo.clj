(ns miner.test-lambdo
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            ;;[miner.lambdo.protocols :refer :all]
            [miner.lambdo :refer :all]))

;; just for hacking, normally you'll want to use a permanent dir
(defn make-tmpdir
  ([] (make-tmpdir "LAMBDO"))
  ([basename] (let [tmp (io/file (System/getenv "TMPDIR") basename ".")]
                (io/make-parents tmp)
                tmp)))


(defn write-stuff [env]
  (is env)
  (let [dbi (open-dbi env)]
    (is dbi)
    ;; auto transactions on each store
    (dbi-store dbi "foo" "bar")
    (dbi-store dbi 1 "one")
    (dbi-store dbi "one" 1)
    (dbi-store dbi '[ka] '[k a])
    (dbi-store dbi :a1 '{:a1 "a1" :A1 '(a 1)})))

(defn read-stuff [env]
  (let [dbi (open-dbi env)
        txn (read-txn env)
        foo (dbi-fetch dbi txn "foo")
        x1 (dbi-fetch dbi txn 1)
        one (dbi-fetch dbi txn "one")
        ka (dbi-fetch dbi txn '[ka])
        a1 (dbi-fetch dbi txn :a1)]
    (txn-reset txn)
    (is (= foo "bar"))
    (is (= x1 "one"))
    (is (= one 1))
    (is (= ka '[k a]))
    (is (= a1 '{:a1 "a1" :A1 '(a 1)}))))

(deftest low-level-test
  (testing "Make an LMDB environment"
    (let [env (create-env (make-tmpdir "LAMBDO_TEST"))]
      (write-stuff env)
      (read-stuff env)
      (.close env))

    ;; re-opening existing database and checking again
    (let [env (create-env (make-tmpdir "LAMBDO_TEST"))]
      (read-stuff env)
      (.close env))))



(deftest simple-test
  (testing "Simple API"
    (let [storage (create-storage (make-tmpdir "LAMBDO_STORAGE"))
          test1 (create-database! storage :test1)]
      (begin! storage)
      (store! test1 :foo "foo")
      (store! test1 :bar 'bar/bar)
      (store! test1 :baz {:a 1 :b 2 :c 3})
      (commit! storage)
      (let [foo (fetch test1 :foo)
            bar (fetch test1 :bar)
            baz (fetch test1 :baz)]
        (begin! storage)
        (let [foo2 (fetch test1 :foo)
              bar2 (fetch test1 :bar)
              baz2 (fetch test1 :baz)]
          (commit! storage)
          (begin! storage)
          (store! test1 :foo "BOOM")
          (rollback! storage)
          (begin! storage)
          (store! test1 :bar 'BOOM)
          (commit! storage)
          (let [foo3 (fetch test1 :foo)
                bar3 (fetch test1 :bar)]
            (close-storage! storage)
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))


