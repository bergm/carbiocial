(ns cluster-test.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn -main
  [rows cols & [append? content]]
  (let [append? (or (edn/read-string append?) false)
        content (or content "x\n")]
    (doseq [r (range (edn/read-string rows)) c (range (edn/read-string cols))
            :let [path-to-file (str "data/row-" r "/col-" c ".txt")]]
      (do
        (io/make-parents path-to-file)
        (spit path-to-file content)))))


#_(-main "10" "10")


