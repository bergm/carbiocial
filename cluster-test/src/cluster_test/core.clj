(ns cluster-test.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn -main
  [& kvs]
  (let [{:keys [rows cols append? content newline?]}
        (reduce (fn [m [k v]]
                  (assoc m (keyword k) v)) {} (partition 2 kvs))

        append? (or (edn/read-string append?) false)
        nl? (edn/read-string newline?)
        newline? (if (nil? nl?) true nl?)
        content (str (or content "x") (when newline? \newline))]
    (doseq [r (range (edn/read-string rows)) c (range (edn/read-string cols))
            :let [path-to-file (str "data/row-" r "/col-" c ".txt")]]
      (do
        (io/make-parents path-to-file)
        (spit path-to-file content :append append?)))))


#_(-main :rows "10" :cols "10" :append? "true" :content "a")


