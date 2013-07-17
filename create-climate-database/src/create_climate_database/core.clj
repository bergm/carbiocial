(ns create-climate-database.core
  (require [clojure.java.jdbc :as jdbc]
           [clojure.java.jdbc.sql :as sql]
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

;skip elements
(def nth-row (dec 254)) ;509
(def nth-col (dec 241)) ;482 964 8 4

(def all-rows (range rows))
(def all-cols (range cols))

(defn insert-raster-points []
  (doseq [row (take-nth nth-row all-rows)]
    (let [h (+ yllcenter (* (- rows row) cellsize))
          iseq (for [col (take-nth nth-col all-cols)]
                 {:id (+ (* row cols)
                         col)
                  :utm21s_r (+ xllcenter
                               (* col cellsize))
                  :utm21s_h h})]
      (apply jdbc/insert! sqlite-db :raster_point iseq)
      (println "row: " row))))

(defn insert-empty-data
  "insert into data table all empty data for the global grid parameters"
  [year]
  (let [all-days (for [doy (range 1 (inc (if (= 0 (mod (- 2012 year) 4)) 366 365)))
                       :let [date (doy-to-date doy)]]
                   {:day (ctc/day date) :month (ctc/month date)})]
    (doseq [row (take-nth nth-row all-rows)]
      (let [h (+ yllcenter
                 (* (- rows row)
                    cellsize))
            iseq (for [col (take-nth nth-col all-cols)
                       {:keys [day month]} all-days]
                   {:raster_point_id (+ (* row cols)
                                        col)
                    :day day :month month :year year})]
        (apply jdbc/insert! sqlite-db :data iseq)
        (println "row: " row)))))

(def climate-elements
  {:globrad {:col :global_radiation_mj_per_m2_times_100
             :scale-factor 100
             :file "gs"}
   :precip {:col :precipitation_mm_times_10
            :scale-factor 10
            :file "p"}
   :windspeed {:col :windspeed_m_per_s_times_10
               :scale-factor 10
               :file "ws"}
   :rel-hum {:col :relative_humidity_percent_times_10
             :scale-factor 10
             :file "rh"}
   :t-min {:col :t_min_times_10
           :scale-factor 10
           :file "tmin"}
   :t-avg {:col :t_avg_times_10
           :scale-factor 10
           :file "t2m_hk"}
   :t-max {:col :t_max_times_10
           :scale-factor 10
           :file "tmax"}})

(defn update-data** [path-to-file day month year into-col-kw scale-factor]
  (println "col: " into-col-kw " scale-factor: " scale-factor " day: " day " month: " month " year: " year)
  (with-open [rdr (io/reader path-to-file)]
    (let [lazy-lines (line-seq rdr)
          ll* (drop 6 lazy-lines)]
      (doseq [[row line] (take-nth nth-row (partition 2 (interleave (range) ll*)))]
        (let [values (edn/read-string (str "[" line "]"))
              iseq (map-indexed (fn [i v]
                                  (let [col (* i nth-col)]
                                    {:where (sql/where {:raster_point_id (+ (* row cols)
                                                                            col)
                                                        :day day
                                                        :month month
                                                        :year year})
                                     :update {into-col-kw (* v scale-factor)}}))
                                (take-nth nth-col values))]
          (doseq [{:keys [where update]} iseq]
            (jdbc/update! sqlite-db :data update where))
          (println "row: " row))))))

(defn update-data* [in-dir year climate-element]
  (let [all-days (for [doy (range 1 (inc 365))
                       :let [date (doy-to-date doy)]]
                   {:day (ctc/day date) :month (ctc/month date)})
        all-days* (map (fn [{:keys [day month] :as m}]
                         (assoc m
                           :sday (str (if (< day 10) "0" "") day)
                           :smonth (str (if (< month 10) "0" "") month)))
                       all-days)
        [col-kw sf file-shortcut] ((juxt :col :scale-factor :file) (climate-element climate-elements))]
    (doseq [{:keys [day month sday smonth]} all-days*]
      #_(println (str in-dir "/" year smonth sday "_" file-shortcut "_sa.asc"))
      (update-data** (str in-dir "/" year smonth sday "_" file-shortcut "_sa.asc") day month year col-kw sf))))

(defn update-data []
  (update-data* "helge_era_interim_data_interpolated_for_target_region/test_2010_gs_asc/test_2010_gs_asc" 2010 :globrad))

