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
    (let [^org.lmdbjava.Env env (create-env (make-tmpdir "LAMBDO_TEST"))]
      (write-stuff env)
      (read-stuff env)
      (.close env))

    ;; re-opening existing database and checking again
    (let [^org.lmdbjava.Env env (create-env (make-tmpdir "LAMBDO_TEST"))]
      (read-stuff env)
      (.close env))))



(deftest simple-test
  (testing "Simple API"
    (let [db (-> (create-ldb (make-tmpdir "LAMBDO_SIMPLE") 10 nil)
                 (create-bin! :test1)
                 (begin!)
                 (store! :test1 :foo "foo")
                 (store! :test1 :bar 'bar/bar)
                 (store! :test1 :baz {:a 1 :b 2 :c 3})
                 (commit!))
          foo (fetch db :test1 :foo)
          bar (fetch db :test1 :bar)
          baz (fetch db :test1 :baz)]
      (let [db (begin! db)
            foo2 (fetch db :test1 :foo)
            bar2 (fetch db :test1 :bar)
            baz2 (fetch db :test1 :baz)
            db (-> db
                   (commit!)
                   (begin!)
                   (store! :test1 :foo "BOOM")
                   (rollback!)
                   (begin!)
                   (store! :test1 :bar 'BOOM)
                   (commit!))
            foo3 (fetch db :test1 :foo)
            bar3 (fetch db :test1 :bar)]
        (close-ldb! db)
        (is (= foo "foo"))
        (is (= bar 'bar/bar))
        (is (= baz {:a 1 :b 2 :c 3}))
        (is (= baz baz2))
        (is (= foo foo2))
        (is (= bar bar2))
        (is (= foo3 foo))
        (is (= bar3 'BOOM)) ))))



;; No atom.  You can't safely use atom, because swap! might retry.  Also, LMDB is not
;; thread-safe. Clojure volatile! is the appropriate thing.  The user takes responsibility
;; for single-threaded usage and doesn't expect atomic updates.

(deftest volatile-test
  (testing "Volatile test"
    (let [db (volatile! (create-ldb (make-tmpdir "LAMBDO_VOLA") 10 nil))]
      (vswap! db create-bin! :test1)
      (vswap! db begin!)
      (vswap! db store! :test1 :foo "foo")
      (vswap! db store! :test1 :bar 'bar/bar)
      (vswap! db store! :test1 :baz {:a 1 :b 2 :c 3})
      (vswap! db commit!)
      (let [foo (fetch @db :test1 :foo)
            bar (fetch @db :test1 :bar)
            baz (fetch @db :test1 :baz)]
        (vswap! db begin!)
        (let [foo2 (fetch @db :test1 :foo)
              bar2 (fetch @db :test1 :bar)
              baz2 (fetch @db :test1 :baz)]
          (vswap! db commit!)
          (vswap! db begin!)
          (vswap! db store! :test1 :foo "BOOM")
          (vswap! db rollback!)
          (vswap! db begin!)
          (vswap! db store! :test1 :bar 'BOOM)
          (vswap! db commit!)
          (let [foo3 (fetch @db :test1 :foo)
                bar3 (fetch @db :test1 :bar)]
            (vswap! db close-ldb!)
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))

;; Ugly but seems to work.  Db is thread-local so it kind of makes sense
(deftest local-var-test
  (testing "local var test"
    (with-local-vars [db (create-ldb (make-tmpdir "LAMBDO_VAR") 10 nil)]
      (var-set db (create-bin! @db :test1))
      (var-set db (begin! @db))
      (var-set db (store! @db :test1 :foo "foo"))
      (var-set db (store! @db :test1 :bar 'bar/bar))
      (var-set db (store! @db :test1 :baz {:a 1 :b 2 :c 3}))
      (var-set db (commit! @db))
      (let [foo (fetch @db :test1 :foo)
            bar (fetch @db :test1 :bar)
            baz (fetch @db :test1 :baz)]
        (var-set db (begin! @db))
        (let [foo2 (fetch @db :test1 :foo)
              bar2 (fetch @db :test1 :bar)
              baz2 (fetch @db :test1 :baz)]
          (var-set  db (commit! @db))
          (var-set  db (begin! @db) )
          (var-set  db (store! @db :test1 :foo "BOOM"))
          (var-set  db (rollback! @db))
          (var-set  db (begin! @db))
          (var-set  db (store! @db :test1 :bar 'BOOM))
          (var-set  db (commit! @db))
          (let [foo3 (fetch @db :test1 :foo)
                bar3 (fetch @db :test1 :bar)]
            (var-set  db (close-ldb! @db))
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))

;; bash in place for a local var
(defmacro bip! [local-var update-fn & args]
  `(var-set ~local-var (~update-fn (var-get ~local-var) ~@args)))


;; same pattern as volatile/atom but with a local var (less overhead, single thread, no
;; sharing)

(deftest bip-local-var-test
  (testing "local var test"
    (with-local-vars [db (create-ldb (make-tmpdir "LAMBDO_BIP") 10 nil)]
      (bip! db create-bin! :test1)
      (bip! db begin!)
      (bip! db store! :test1 :foo "foo")
      (bip! db store! :test1 :bar 'bar/bar)
      (bip! db store! :test1 :baz {:a 1 :b 2 :c 3})
      (bip! db commit!)
      (let [foo (fetch @db :test1 :foo)
            bar (fetch @db :test1 :bar)
            baz (fetch @db :test1 :baz)]
        (bip! db begin!)
        (let [foo2 (fetch @db :test1 :foo)
              bar2 (fetch @db :test1 :bar)
              baz2 (fetch @db :test1 :baz)]
          (bip! db commit!)
          (bip! db begin!)
          (bip! db store! :test1 :foo "BOOM")
          (bip! db rollback!)
          (bip! db begin!)
          (bip! db store! :test1 :bar 'BOOM)
          (bip! db commit!)
          (let [foo3 (fetch @db :test1 :foo)
                bar3 (fetch @db :test1 :bar)]
            (bip! db close-ldb!)
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))




;; Another ugly way to use it.  Not recommended.
(def ^:dynamic *db* nil)

(deftest def-var-test
  (testing "dynamic var test"
    (binding [*db* (create-ldb (make-tmpdir "LAMBDO_VAR2") 10 nil)]
      (set! *db* (create-bin! *db* :test1))
      (set! *db* (begin! *db*))
      (set! *db* (store! *db* :test1 :foo "foo"))
      (set! *db* (store! *db* :test1 :bar 'bar/bar))
      (set! *db* (store! *db* :test1 :baz {:a 1 :b 2 :c 3}))
      (set! *db* (commit! *db*))
      (let [foo (fetch *db* :test1 :foo)
            bar (fetch *db* :test1 :bar)
            baz (fetch *db* :test1 :baz)]
        (set! *db* (begin! *db*))
        (let [foo2 (fetch *db* :test1 :foo)
              bar2 (fetch *db* :test1 :bar)
              baz2 (fetch *db* :test1 :baz)]
          (set!  *db* (commit! *db*))
          (set!  *db* (begin! *db*) )
          (set!  *db* (store! *db* :test1 :foo "BOOM"))
          (set!  *db* (rollback! *db*))
          (set!  *db* (begin! *db*))
          (set!  *db* (store! *db* :test1 :bar 'BOOM))
          (set!  *db* (commit! *db*))
          (let [foo3 (fetch *db* :test1 :foo)
                bar3 (fetch *db* :test1 :bar)]
            (set!  *db* (close-ldb! *db*))
            (is (= foo "foo"))
            (is (= bar 'bar/bar))
            (is (= baz {:a 1 :b 2 :c 3}))
            (is (= baz baz2))
            (is (= foo foo2))
            (is (= bar bar2))
            (is (= foo3 foo))
            (is (= bar3 'BOOM))))))))
