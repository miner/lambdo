(defproject com.velisco/lambdo "0.4.5-SNAPSHOT"
  :description "Lambdo, Clojure key/value database based on LMDB"
  :url "https://github.com/miner/lambdo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ;; [org.clojure/data.fressian "0.2.1-SNAPSHOT"]
                 [datascript "0.18.8"]
                 [net.openhft/zero-allocation-hashing "0.10.1"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.lmdbjava/lmdbjava "0.7.0"]
                 ])
