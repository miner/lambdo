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


(deftest simple-test
  (testing "Simple API"
    (let [pathname (make-tmpdir (str "LAMBDO_TEST_" (System/currentTimeMillis)))
          database (create-database! pathname)
          test1 (create-bucket! database :test1)]
      (println "simple-test path:" (str pathname))
      (begin! database)
      (assoc! test1 :aaaaa "five")
      (assoc! test1 :foob "foo")
      (assoc! test1 :bar 'bar/bar)
      (assoc! test1 :baz {:a 1 :b 2 :c 3})
      (commit! database)
      (let [foo (get test1 :foob)
            bar (get test1 :bar)
            baz (get test1 :baz)]
        (begin! database)
        (let [foo2 (:foob test1)
              bar2 (:bar test1)
              baz2 (:baz test1)]
          (commit! database)
          (begin! database)
          (assoc! test1 :foob "BOOM")
          (rollback! database)
          (begin! database)
          (assoc! test1 :bar 'BOOM)
          (commit! database)
          (let [foo3 (get test1 :foob)
                bar3 (get test1 :bar)
                t1-str-vals (transduce (comp (map val) (filter string?)) conj [] test1)
                t1-trans-keys (transduce (map key) conj [] test1)
                t1-trans-rev-keys (transduce (map key) conj () test1)
                t1-keys (into [] (reducible test1 :keys-only? true))
                t1-second-keys (into [] (reducible test1 :keys-only? true :step 2))
                t1-rev-second-keys (into [] (reducible test1 :keys-only? true :step -2))
                t1-second-keys-from-bar (into [] (reducible test1 :keys-only? true :start
                                                            :bar :step 2))
                t1-second-keys-to-bar (into [] (reducible test1 :keys-only? true :end
                                                            :bar :step -2))                
                t1-rev-second-keys (into [] (reducible test1 :keys-only? true :step -2))
                t1-rev-keys (transduce (map key) conj [] (reducible test1 :step -1))
                t1-pre-bazz (into [] (reducible test1 :end :bazz :step -1))
                all-test1 (reduce conj {} test1)
                rev-test1 (reduce-kv conj [] (reducible test1 :step -1))
                fob-test-end (reduce-kv conj [] (reducible test1 :end :dobzy))
                fob-test1 (reduce-kv conj [] (reducible test1 :start :dobzy))]
            (close-database! database)
            (is (= t1-str-vals ["five" "foo"]))
            (is (= t1-keys) [:aaaaa :bar :baz :foob])
            (is (= t1-second-keys-to-bar) [:foob :bar])
            (is (= t1-second-keys-from-bar) [:bar :foob])
            (is (= t1-rev-second-keys) [:foob :bar])
            (is (= t1-second-keys) [:aaaaa :baz])
            (is (= t1-keys t1-trans-keys))
            (is (= t1-rev-keys t1-trans-rev-keys))
            (is (= t1-rev-keys) [:foob :baz :bar :aaaaa])
            (is (= t1-pre-bazz) [:baz :bar :aaaaa])
            (is (= all-test1 '{:aaaaa "five" :bar BOOM :baz {:a 1, :b 2, :c 3} :foob "foo"}))
            (is (= rev-test1 '[:foob "foo" :baz {:a 1, :b 2, :c 3} :bar BOOM :aaaaa "five"]))
            (is (= fob-test-end '[:aaaaa "five" :bar BOOM :baz {:a 1, :b 2, :c 3}]))
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
          database (create-database! pathname)
          test2 (create-bucket! database :test2)]
      (begin! database)
      (assoc! test2 :a 11 :b 22 :c 33 :d 44)
      (commit! database)
      ;; transaction detour
      (begin! database)
      (assoc! test2 :a 101)
      (dissoc! test2 :b)
      (is (nil? (:b test2)))
      (is (= 101 (:a test2)))
      (rollback! database)
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
        (close-database! database)
        (is (= pnil lk))
        (is (= nnil fk))
        (is (= ap nil))
        (is (= an :b))
        (is (= dp :c))
        (is (= dn nil))
        (is (= fk :a))
        (is (= lk :d))))))


(deftest reducible-steps
  (testing "Reducibles and steps"
    (let [pathname (make-tmpdir (str "TEST_REDSTEPS_" (System/currentTimeMillis)))
          database (create-database! pathname)
          bbb (create-bucket! database :bbb {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7})]
      (println "reducibles-test path:" (str pathname))
      (is (= (seq (reducible bbb :start :a :end :e :keys-only? true))
             '(:a :b :c :d :e)))
      (is (= (seq (reducible bbb :start :a :end :e :keys-only? true :step 2)) 
             '(:a :c :e)))
      (is (= (seq (reducible bbb :start nil :step -3 :keys-only? true))
             '(:g :d :a)))
      (is (= (seq (reducible bbb :start :b :step 2 :keys-only? true))
             '(:b :d :f)))
      (is (= (seq (reducible bbb :start :f :step -3 :keys-only? true))
             '(:f :c)))
      (is (= (seq (reducible bbb :start :f :end :d :step -2))
             '([:f 6] [:d 4])))
      (is (= (seq (reducible bbb :start :a :end :f :step 2))
             '([:a 1] [:c 3] [:e 5])))
      (close-database! database)
      )))

(defn kwpad [n]
  (let [ktemp "k0000000000"
        klen (.length ktemp)
        nstr (str n)
        nlen (.length nstr)]
    (keyword (str (subs ktemp 0 (- klen nlen)) nstr))))

(deftest larger-test
  (testing "Larger stuff"
    (let [size 10000
          pathname (make-tmpdir (str "LAMBDO_TEST_LARGE_" (System/currentTimeMillis)))
          database (create-database! pathname)
          snapshot (reduce (fn [res n] (assoc res (kwpad n) n)) (sorted-map) (range size))
          test2 (create-bucket! database :test2 snapshot)]
      (println "larger-test path:" (str pathname))
      (begin! database)
      (is (= (into [] (reducible test2 :keys-only? true :step 1000))
             (map kwpad (range 0 size 1000))))
      (is (= (into [] (reducible test2 :keys-only? true :step -1000))
             (map kwpad (range (dec size) -1 -1000))))
      (is (= (into [] (reducible test2 :keys-only? true :start (kwpad 100) :step 1000))
             (map kwpad (range 100 size 1000))))
      (is (= (into [] (reducible test2 :keys-only? true :start (kwpad 9000) :step -1000))
             (map kwpad (range 9000 -1 -1000))))
      (is (= (into [] (reducible test2 :keys-only? true :end (kwpad 100) :start (kwpad 9000)
                                 :step -1000))
             (map kwpad (range 9000 99 -1000))))
      (is (= (reduce-kv (fn [r _k v] (+ r v)) 0 test2)   (reduce + (range size))))
      (is (= (reduce-kv (fn [r _k v] (+ r v)) 0 (reducible test2 :start (kwpad 9000)
                                                          :end (kwpad 100) :step -1000))
             (reduce + 0 (range 9000 99 -1000))))

      )))
