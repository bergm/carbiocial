(ns cluster-test.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn write-files-test
  [{:keys [data-dir rows cols append? content newline?]}]
  (let [data-dir (or (edn/read-string data-dir) "data")
        append? (or (edn/read-string append?) false)
        nl? (edn/read-string newline?)
        newline? (if (nil? nl?) true nl?)
        content (str (or content "x") (when newline? \newline))]
    (doseq [r (range (edn/read-string rows)) c (range (edn/read-string cols))
            :let [path-to-file (str data-dir "/row-" r "/col-" c ".txt")]]
      (do
        (when-not append? (io/make-parents path-to-file))
        (spit path-to-file content :append append?)))))

(defn read-files-test
  [{:keys [data-dir rows cols]}]
  (let [data-dir (or (edn/read-string data-dir) "data")]
    (doseq [r (range (edn/read-string rows)) c (range (edn/read-string cols))
            :let [path-to-file (str data-dir "/row-" r "/col-" c ".txt")]]
      (slurp path-to-file))))

(defn -main
  [& kvs]
  (let [options (reduce (fn [m [k v]]
                          (assoc m (keyword k) v))
                        {} (partition 2 kvs))]
    (case (:test options)
      "read" (doseq [t (range 1 (edn/read-string (or (:count options) "4")))]
		(.start (Thread. (partial read-files-test options #_(assoc options :data-dir (str "data-" t))))))
      "write" (write-files-test options)
      nil (write-files-test options))))

#_(-main :rows "10" :cols "10" :append? "true" :content "a")




