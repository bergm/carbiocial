(ns create-soil-database.core
  (require [korma
            [db :as kdb]
            [core :as kc]]
           [clojure.java.jdbc :as j]
           [clojure.java.jdbc.sql :as s]
           [clojure.edn :as edn]
           [clojure-csv.core :as csv]
           [clojure.algo.generic.functor :as gf]))

(def cols {:lat :lat_times_10000
           :lng :lng_times_10000
           :h-id :horizon_id
           :soil-class :soil_class
           :soil-class-id :soil_class_id
           :soil-depth :original_soil_profile_depth_cm
           :h-symb :horizon_symbol
           :upper :upper_horizon_cm
           :lower :lower_horizon_cm
           :clay :clay_percent
           :silt :silt_percent
           :sand :sand_percent
           :ph :ph_kcl
           :c :c_org_percent
           :c-n :c_n
           :bd :bulk_density_t_per_m3})

(def sqlite-db (kdb/sqlite3 {:db "carbiocial.sqlite"}))

#_(kdb/defdb db (kdb/sqlite3 {:db "carbiocial.sqlite"}))

(def file (slurp "MTSoilDB_Carbiocial_0-2_result_mod.txt"))

(def parsed-csv (csv/parse-csv file :delimiter \tab))

(defn create-insert-seq* [csv]
  (for [[lat lng soil-class soil-class-2 soil-depth hz-simb upper lower hz-id silt clay sand ph c c-n bd] csv]
    {(:lat cols) (int (* (edn/read-string lat) 10000))
     (:lng cols) (int (* (edn/read-string lng) 10000))
     (:h-id cols) (edn/read-string hz-id)
     (:soil-class cols) (edn/read-string soil-class)
     :soil-class* (edn/read-string soil-class-2)
     (:soil-depth cols) (edn/read-string soil-depth)
     (:h-symb cols) (edn/read-string hz-simb)
     (:upper cols) (edn/read-string upper)
     (:lower cols) (edn/read-string lower)
     (:silt cols) (edn/read-string silt)
     (:clay cols) (edn/read-string clay)
     (:sand cols) (edn/read-string sand)
     (:ph cols) (edn/read-string ph)
     (:c cols) (edn/read-string c)
     (:c-n cols) (edn/read-string c-n)
     (:bd cols) (edn/read-string bd)}))

(defn soil-classes-to-id [iseq*]
  (reduce (fn [a sc]
            (if (contains? a sc)
              a
              (assoc a sc (count a))))
          {} (map :soil-class* iseq*)))

(defn soil-classes-iseq [sc-to-id]
   (map (fn [[sc id]] {:id id :name sc}) sc-to-id))

(defn create-insert-seq [sc-to-id iseq*]
  (map (fn [m]
         (-> m
             (assoc ,,, (:soil-class-id cols) (get sc-to-id (:soil-class* m)))
             (dissoc ,,, :soil-class*)))
       iseq*))

(def insert-seq* (create-insert-seq* (rest parsed-csv)))
(def insert-seq (create-insert-seq (soil-classes-to-id insert-seq*)
                                   insert-seq*))

(defn do-insert-at-once [table-kw iseq]
  "insert into database"
  (apply j/insert! sqlite-db table-kw iseq))



(defn do-check [iseq]
  (let [errors (atom [])
        success (reduce (fn [m iv]
                          (let [k (select-keys iv [(:lat cols) (:lng cols) (:h-id cols)])
                                v (select-keys iv (apply disj (into #{} (keys iv)) (keys k)))]
                            (if (not (find m k))
                              (assoc m k v)
                              (do
                                (swap! errors conj k)
                                m))))
                        {} iseq)]
    [#(-> success) @errors]))

(defn overlapping-depths [iseq]
  (let [m (reduce (fn [m v]
                    (let [k (select-keys v [(:lat cols) (:lng cols)])
                          hid ((:h-id cols) v)
                          m* (assoc m k (get m k (sorted-map)))]
                      (assoc-in m* [k hid] {:upper ((:upper cols) v)
                                            :lower ((:lower cols) v)})))
                  {} iseq)
        ;_ (println "at m2: " (first m))
        m3 (gf/fmap (partial partition 2 1) m)
        ;_ (println "at m3: " (first m3))
        m4 (into {} (map (fn [[k v]]
                           [k (map (fn [[[_ {u1 :upper l1 :lower}] [_ {u2 :upper l2 :lower}] :as v*]]
                                     (if (= (- u2 l1) 1)
                                       :ok
                                       v*))
                                   v)])
                         m3))
        non-ok (filter (comp (partial not-every? #{:ok}) second) m4)]
    non-ok))

(defn flatten-2-level-map [two-level-map]
  (flatten
   (for [[k v] two-level-map]
     (for [[k* v*] v]
       (-> v*
           (merge ,,, k)
           (assoc ,,, (:h-id cols) k*))))))

(defn correct-overlapping-depths [iseq errors]
  (let [m (reduce (fn [m v]
                    (let [k (select-keys v [(:lat cols) (:lng cols)])
                          hid ((:h-id cols) v)
                          m* (assoc m k (get m k (sorted-map)))]
                      (assoc-in m* [k hid] (select-keys v (disj (into #{} (keys v)) #{(:lat cols) (:lng cols) (:h-id cols)})))))
                  {} iseq)

        m2 (gf/fmap (fn [v]
                      (second
                       (reduce (fn [[id* v] [id v*]]
                                 (if (= 1 (- id id*))
                                   [id v]
                                   [(inc id*) (assoc (dissoc v id) (inc id*) v*)]))
                               [0 v] v)))
                    m)

        m3 (reduce (fn [m [ek _]]
                     (assoc-in m [ek 2]
                               (assoc (get-in m [ek 2]) (:upper cols) (inc (get-in m [ek 1 (:lower cols)])))))
                   m2 errors)
        ]
    (flatten-2-level-map m3)))

(defn do-check-horizon-depths [iseq]
  (dorun (map (fn [[k v]] (println k v)) (overlapping-depths iseq))))

(defn do-check-horizon-depths* [iseq]
  (dorun (map (fn [[k v]] (println k v))
              (-> iseq
                  (correct-overlapping-depths ,,, (overlapping-depths iseq))
                  overlapping-depths))))

#_(defn do-insert-corrected [iseq]
  (dorun
   (for [ivalue (correct-overlapping-depths iseq (overlapping-depths iseq))]
     (do
       (println "lat: " ((:lat cols) ivalue) " lng: " ((:lng cols) ivalue) " id: " ((:h-id cols) ivalue))
       (kc/insert soil-data (kc/values [ivalue]))))))

#_(defn do-insert [iseq]
  (dorun
   (for [ivalue iseq]
     (do
       (println "lat: " ((:lat cols) ivalue) " lng: " ((:lng cols) ivalue) " id: " ((:h-id cols) ivalue))
       (kc/insert soil-data (kc/values [ivalue]))))))

#_(defn do-insert-at-once []
  (kc/insert soil-data (kc/values insert-seq)))



#_(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [file (slurp "MTSoilDB_Carbiocial_0-1_result.txt")
        parsed-csv (csv/parse-csv file :delimiter \tab)

        insert-seq (create-insert-seq (rest parsed-csv))]
    (kc/insert soil-data (kc/values insert-seq))))
