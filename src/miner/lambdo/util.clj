(ns miner.lambdo.util
  (:import java.nio.charset.StandardCharsets))


;; reflection to get private field
(defn private-field [^Object obj ^String field-name]
  (let [field (.getDeclaredField (.getClass obj) field-name)]
    (.setAccessible field true)
    (.get field obj)))

(defn str->utf8 [s]
  (when s
    (.getBytes ^String s StandardCharsets/UTF_8)))

(defn utf8->str [bs]
  (when bs
    (String. ^bytes bs StandardCharsets/UTF_8)))


