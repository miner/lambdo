(ns miner.test-lambdo
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [miner.lambdo.impl :refer :all]
            [miner.lambdo :refer :all]))

;; just for hacking, normally you'll want to use a permanent dir
(defn make-tmpdir
  ([] (make-tmpdir "LAMBDO"))
  ([basename] (let [tmp (io/file (System/getenv "TMPDIR") (str basename) ".")]
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
    (let [pathname (make-tmpdir (str "LAMBDO_TEST_" (System/currentTimeMillis)))
          storage (create-storage! pathname)
          test1 (create-bucket! storage :test1)]
      (println "simple-test path:" (str pathname))
      (begin! storage)
      (assoc! test1 :aaaaa "five")
      (assoc! test1 :foob "foo")
      (assoc! test1 :bar 'bar/bar)
      (assoc! test1 :baz {:a 1 :b 2 :c 3})
      (commit! storage)
      (let [foo (get test1 :foob)
            bar (get test1 :bar)
            baz (get test1 :baz)]
        (begin! storage)
        (let [foo2 (:foob test1)
              bar2 (:bar test1)
              baz2 (:baz test1)]
          (commit! storage)
          (begin! storage)
          (assoc! test1 :foob "BOOM")
          (rollback! storage)
          (begin! storage)
          (assoc! test1 :bar 'BOOM)
          (commit! storage)
          (let [foo3 (get test1 :foob)
                bar3 (get test1 :bar)
                t1-str-vals (transduce (comp (map val) (filter string?)) conj [] test1)
                t1-trans-keys (transduce (map key) conj [] test1)
                t1-trans-rev-keys (transduce (map key) conj () test1)
                t1-keys (into [] (reducible test1 :keys-only? true))
                t1-rev-keys (transduce (map key) conj [] (reducible test1 :reverse? true))
                t1-pre-bazz (into [] (reducible test1 :start :bazz :reverse? true))
                all-test1 (reduce conj {} test1)
                rev-test1 (reduce-kv conj [] (reducible test1 :reverse? true))
                fob-test1 (reduce-kv conj [] (reducible test1 :start :dobzy))]
            (close-storage! storage)
            (is (= t1-str-vals ["five" "foo"]))
            (is (= t1-keys) [:aaaaa :bar :baz :foob])
            (is (= t1-keys t1-trans-keys))
            (is (= t1-rev-keys t1-trans-rev-keys))
            (is (= t1-rev-keys) [:foob :baz :bar :aaaaa])
            (is (= t1-pre-bazz) [:baz :bar :aaaaa])
            (is (= all-test1 '{:aaaaa "five" :bar BOOM :baz {:a 1, :b 2, :c 3} :foob "foo"}))
            (is (= rev-test1 '[:foob "foo" :baz {:a 1, :b 2, :c 3} :bar BOOM :aaaaa "five"]))
            (is (= fob-test1 [:foob "foo"]))
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))

(deftest key-nav-test
  (testing "Key Nav API"
    (let [pathname (make-tmpdir (str "LAMBDO_KTEST_" (System/currentTimeMillis)))
          storage (create-storage! pathname)
          test2 (create-bucket! storage :test2)]
      (begin! storage)
      (assoc! test2 :a 11 :b 22 :c 33 :d 44)
      (commit! storage)
      ;; transaction detour
      (begin! storage)
      (assoc! test2 :a 101)
      (dissoc! test2 :b)
      (is (nil? (:b test2)))
      (is (= 101 (:a test2)))
      (rollback! storage)
      ;; confirm rollback
      (is (= (:b test2) 22))
      ;; back to key nav
      (let [ap (previous-key test2 :a)
            an (next-key test2 :a)
            dp (previous-key test2 :d)
            dn (next-key test2 :d)
            fk (first-key test2)
            lk (last-key test2)
            pnil (previous-key test2 nil)
            nnil (next-key test2 nil)]
        (close-storage! storage)
        (is (= pnil lk))
        (is (= nnil fk))
        (is (= ap nil))
        (is (= an :b))
        (is (= dp :c))
        (is (= dn nil))
        (is (= fk :a))
        (is (= lk :d))))))

