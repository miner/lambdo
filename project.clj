(defproject com.velisco/lambdo "0.4.7-SNAPSHOT"
  :description "Lambdo, Clojure key/value database based on LMDB"
  :url "https://github.com/miner/lambdo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 ;; [org.clojure/data.fressian "0.2.1-SNAPSHOT"]
                 [datascript "1.5.4"]
                 [net.openhft/zero-allocation-hashing "0.16"]
                 [com.taoensso/nippy "3.3.0"]
                 ;;[org.lmdbjava/lmdbjava "0.8.0"]
                 [org.lmdbjava/lmdbjava "0.9.0"]
                 ])

;; need to set env
;; JAVA_HOME  /Library/Java/JavaVirtualMachines/amazon-corretto-8.jdk/Contents/Home

;; snapshots of lmbdjava
;; https://oss.sonatype.org/content/repositories/snapshots/

