(defproject com.velisco/lambdo "0.4.1-SNAPSHOT"
  :description "Lambdo, Clojure key/value database based on LMDB"
  :url "https://github.com/miner/lambdo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 ;; [org.clojure/data.fressian "0.2.1-SNAPSHOT"]
                 [net.openhft/zero-allocation-hashing "0.8"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.lmdbjava/lmdbjava "0.6.1"]])
