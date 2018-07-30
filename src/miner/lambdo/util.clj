(ns miner.lambdo.util
  (:require [taoensso.nippy :as nip])
  (:import java.nio.charset.StandardCharsets
           java.security.MessageDigest
           net.openhft.hashing.LongHashFunction))


;; reflection to get private field
(defn private-field [^Object obj ^String field-name]
  (let [field (.getDeclaredField (.getClass obj) field-name)]
    (.setAccessible field true)
    (.get field obj)))

(defn str->utf8 ^bytes [^String s]
  (when s
    (.getBytes s StandardCharsets/UTF_8)))

(defn utf8->str ^String [^bytes bs]
  (when bs
    (String. bs StandardCharsets/UTF_8)))

;; SHA is 20 bytes
(let [sha1 ^MessageDigest (MessageDigest/getInstance "SHA")]
  (defn sha ^bytes [x]
    (.digest ^MessageDigest sha1 (str->utf8 (pr-str x)))))

(defn sha-str [x]
  (apply str "#SHA-" (map #(format "%02X" %) (seq (sha x)))))


;; xxhash returns a long digest

;; We're going to need a ByteBuffer anyway to store the value, so we might as well hash the
;; ByteBuffer.   xx.hashBytes(buf).  Actually, we can get the byte-array from nippy and then
;; use that for xxhash and making the ByteBuffer.
;;
;; But other times we might be holding the Clojure value and need to check.  Does ByteBuffer
;; have an equals?  Maybe we don't need to decode the value if we encode it into the BB????


;; Nippy makes it super fast.  Order of magnitude better than getting bytes from pr-str.
;; But, of course, much slower than `hash`.  Remember, though, that hash is not guaranteed
;; to be stable for different runs of an application.  We have to be consistent across
;; processes, JVMs, and machines.  xxhash/nippy looks pretty good -- and simple for me.

(defn xxhash-bytes ^long [^bytes ba]
  (.hashBytes ^LongHashFunction (LongHashFunction/xx) ba))

(defn xxhash ^long [x]
  (xxhash-bytes ^bytes (nip/fast-freeze x)))
