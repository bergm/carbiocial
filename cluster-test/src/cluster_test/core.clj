(ns cluster-test.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [clj-time.core :as ctc]
            [clojure-csv.core :as csv]
            [clj-time.coerce :as ctcoe]
            [clj-time.format :as ctf]))

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
      "read" (read-files-test options)
      "write" (write-files-test options)
      nil (write-files-test options))))

#_(-main :rows "10" :cols "10" :append? "true" :content "a")


(defn make-file-name [path sym d m y]
  (str path y (when (< m 10) 0) m (when (< d 10) 0) d "_" (name sym) "_sa.asc"))

(def opened-files (atom {}))
(def line-seqs (atom {}))

(defn open-files []
  (doseq [sym [:gs :ws :t2m_hk :tmin :tmax :p :rh]
          year (range 1981 1982 #_2013)
          diy (range 1 3 #_366)
          :let [date (ctc/plus (ctc/date-time year 1 1) (ctc/days (dec diy)))
                date* (ctcoe/to-long date)
                [day month] ((juxt ctc/day ctc/month) date)
                rdr (clojure.java.io/reader (make-file-name (str "g:/" (name sym) "_1981-2013/") sym day month year))]]
    (swap! opened-files assoc-in [sym date*] rdr)
    (swap! line-seqs assoc-in [sym date*] (line-seq rdr))))


(defn close-files []
  (doseq [[sym rdrs] @opened-files
          [_ rdr] rdrs]
    (.close rdr)))


(defn create-row-strs [{:keys [tmin t2m_hk tmax p rh ws gs] :as row}]
  (map (fn [[date* tmin] [_ tavg] [_ tmax] [_ p] [_ gs] [_ rh] [_ ws]]
         (let [date (ctcoe/from-long date*)
               date-str (ctf/unparse (ctf/formatter "dd.MM.yyyy") date)]
           (map str [(ctc/day date) (ctc/month date) (ctc/year date) date-str tmin tavg tmax p gs rh ws])))
       tmin t2m_hk tmax p gs rh ws))

(defn drop-header []
  (doseq [[sym lseqs] @line-seqs
          [date* lseq] lseqs]
    (swap! line-seqs update-in [sym date*] #(drop 6 %))))

(def fmap
  (fn [f m]
    (into (sorted-map)
          (map (fn [[k v]]
                 [k (f v)])
               m))))

(def csv-header
  ["day" "month" "year" "date" "tmin" "tavg" "tmax" "precip" "globrad" "relhumid" "windspeed"])

(defn write-climate-files []
  (loop [rows @line-seqs
         row-count 0] ;loop over rows in all files
    (if (or (= row-count 3) (-> rows :gs first second nil?))
      :ready
      (let [row (fmap #(fmap (fn [v] (-> v first (cs/split ,,, #"\s+"))) %) rows)]
        (loop [cols row
               col-count 0] ;loop over the cols in all files
          (if (-> cols :gs first second nil?)
            :ready
            (let [row-strs (create-row-strs (fmap #(fmap first %) cols))
                  path-to-file (str "data/row-" row-count "/col-" col-count ".asc")
                  _ (io/make-parents path-to-file)]
              (with-open [w (clojure.java.io/writer path-to-file :append true)]
                (.write w (csv/write-csv [csv-header]))
                (doseq [row-str row-strs]
                  (.write w (csv/write-csv [row-str]))))
              (recur (fmap #(fmap next %) cols) (inc col-count)))))
        (recur (fmap #(fmap next %) rows) (inc row-count))))))

(defn run-climate-file-conversion []
  (open-files)
  (drop-header)
  (write-climate-files)
  (close-files))

#_(loop [a 10]
  (if (= a 0)
    :ready
    (do
      (println "a: " a)
      (loop [b a]
        (if (= b 0)
          :bli
          (do
            (println "b: " b)
            (recur (dec b)))))
      (recur (dec a))))
  )



#_(defn write-file []

  (with-open [w (clojure.java.io/writer  "f:/w.txt" :append true)]

    (.write w (str "hello" "world"))))





#_(close-files)

#_(with-open [rdr (clojure.java.io/reader (make-file-name "g:/gs_1981-2013/" :gs 1 1 1981))]
  (count (line-seq rdr)))








