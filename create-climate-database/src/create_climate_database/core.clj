(ns create-climate-database.core
  (require [clojure.java.jdbc :as jdbc]
           #_[clojure.java.jdbc.sql :as sql]
           [clojure.edn :as edn]
           #_[clojure.algo.generic.functor :as gf]
           [clojure.java.io :as io]
           [clj-time.core :as ctc]))

(defn doy-to-date [doy]
  (ctc/plus (ctc/date-time 2010 1 1) (ctc/days (dec doy))))

(def sqlite-db {:db "carbiocial-climate.sqlite"
                :classname "org.sqlite.JDBC"
                :subprotocol "sqlite"
                :subname "carbiocial-climate.sqlite"
                :make-pool? true})

(def rows 2545)
(def cols 1928)
(def xllcenter -8895)
(def yllcenter 8001115)
(def cellsize 900)
(def nodata -99999.00)

(def nth-row (dec 509))
(def nth-col (dec 241)) ;482 964 8 4

(defn insert-raster-points* [path-to-file]
  (with-open [rdr (io/reader path-to-file)]
    (let [lazy-lines (line-seq rdr)
          ll* (drop 6 lazy-lines)]
      (doseq [[row line] (take-nth nth-row (partition 2 (interleave (range) ll*)))]
        (let [values (edn/read-string (str "[" line "]"))
              h (+ yllcenter (* (- rows row) cellsize))
              iseq (map-indexed (fn [i _]
                                  (let [col (* i nth-col)]
                                    {:id (+ row col)
                                     :utm21s_r (+ xllcenter (* col cellsize))
                                     :utm21s_h h}))
                                (take-nth nth-col values))]
          (apply jdbc/insert! sqlite-db :raster_point iseq)
          (println "row: " row))))))

(def insert-raster-points []
  (insert-raster-points* "helge_era_interim_data_interpolated_for_target_region/test_2010_gs_asc/test_2010_gs_asc/20100101_gs_sa.asc"))

(defn insert-empty-data* [path-to-file year]
  (with-open [rdr (io/reader path-to-file)]
    (let [lazy-lines (line-seq rdr)
          ll* (drop 6 lazy-lines)
          all-days (for [doy (range 1 (inc (if (= 0 (mod (- 2012 year) 4)) 366 365)))
                         :let [date (doy-to-date doy)]]
                     {:day (ctc/day date) :month (ctc/month date)})]
      (doseq [[row line] (take-nth nth-row (partition 2 (interleave (range) ll*)))]
        (let [values (edn/read-string (str "[" line "]"))
              h (+ yllcenter (* (- rows row) cellsize))
              iseq (map-indexed (fn [i v]
                                  (let [col (* i nth-col)]
                                    {:raster_point_id (+ row col)
                                     :day 1 :month 1 :year 2010}))
                                (take-nth nth-col values))
              iseq* (for [raster-point iseq
                          {:keys [day month]} all-days]
                      (assoc raster-point :day day :month month))]
          (apply jdbc/insert! sqlite-db :data iseq*)
          (println "row: " row))))))

(def climate-elements
  {:globrad {:col :global_radiation_times_100
             :scale-factor 100}
   :precip {:col :precipitation_mm
            :scale-factor 1}
   :windspeed {:col :windspeed
               :scale-factor 1}
   :rel-hum {:col :relative-humidity
             :scale-factor 1}
   :t-min {:col :t_min_times_10
           :scale-factor 10}
   :t-avg {:col :t_avg_times_10
           :scale-factor 10}
   :t-max {:col :t_max_times_10
           :scale-factor 10}})

(defn insert-data* [path-to-file day month year into-col-kw scale-factor]
  (with-open [rdr (io/reader path-to-file)]
    (let [lazy-lines (line-seq rdr)
          ll* (drop 6 lazy-lines)]
      (doseq [[row line] (take-nth nth-row (partition 2 (interleave (range) ll*)))]
        (let [values (edn/read-string (str "[" line "]"))
              h (+ yllcenter (* (- rows row) cellsize))
              iseq (map-indexed (fn [i v]
                                  (let [col (* i nth-col)]
                                    {:where (jdbc/where {:raster_point_id (+ row col)
                                                         :day day
                                                         :month month
                                                         :year year})
                                     :update {into-col-kw (* v scale-factor)}}))
                                (take-nth nth-col values))]
          (doseq [{:keys [where update]} iseq]
            (apply jdbc/insert! sqlite-db :data update where))
          (println "row: " row))))))

(def insert-data** [in-dir year climate-element]
  (let [all-days (for [doy (range 1 (inc 365))
                       :let [date (doy-to-date doy)]]
                   {:day (ctc/day date) :month (ctc/month date)})
        [col-kw sf] (juxt :col :scale-factor (climate-elements climate-element))]
    (doseq [{:keys [day month]} all-days]
      (insert-data* (io/file (str in-dir "/" year (ctc/month date) (ctc/day date) "_" climate-element "_sa.asc")) day month year col-kw sf))))

(def insert-data []
  (insert-data* "helge_era_interim_data_interpolated_for_target_region/test_2010_gs_asc/test_2010_gs_asc/" 2010 :globrad))

