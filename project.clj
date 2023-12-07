(defproject com.velisco/lambdo "0.4.8-SNAPSHOT"
  :description "Lambdo, Clojure key/value database based on LMDB"
  :url "https://github.com/miner/lambdo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots/"}
  :dependencies [[org.clojure/clojure "1.12.0-alpha5"]
                 ;; [org.clojure/data.fressian "0.2.1-SNAPSHOT"]
                 [datascript "1.5.4"]
                 [net.openhft/zero-allocation-hashing "0.16"]
                 [com.taoensso/nippy "3.3.0"]
                 ;;[org.lmdbjava/lmdbjava "0.8.0"]
                 [org.lmdbjava/lmdbjava "0.9.0"]
                 ]
  ;; these options work only with Java 9+, works around a problem with lmdbjava
   :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
              "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"] )


;; need to set env
;; JAVA_HOME  /Library/Java/JavaVirtualMachines/amazon-corretto-8.jdk/Contents/Home

;;; later versions of Java (9+) need explicit settings to allow "open" access.

;; snapshots of lmbdjava
;; https://oss.sonatype.org/content/repositories/snapshots/

;;; https://stackoverflow.com/questions/73872389/java-lang-reflect-inaccessibleobjectexception-unable-to-make-field-private-fina

;;; Unable to make field long java.nio.Buffer.address accessible: module java.base does not
;;; "opens java.nio" to unnamed module @3359c0cf
;;;
;;; --add-opens=java.base/java.nio=ALL-UNNAMED
;;; --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
