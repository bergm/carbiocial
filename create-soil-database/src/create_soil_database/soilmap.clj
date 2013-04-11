(ns create-soil-database.soilmap
  (require [korma 
            [db :as kdb]
            [core :as kc]]
           [clojure-csv.core :as csv]
           [clojure.edn :as edn]
           [clojure.algo.generic.functor :as gf]
           [clojure.java.jdbc :as j]
           [clojure.java.jdbc.sql :as s]))

(def sqlite-db (kdb/sqlite3 {:db "carbiocial.sqlite"})
  #_{:db "carbiocial.sqlite"
     :classname "org.sqlite.JDBC"
     :subprotocol "sqlite"
     :subname "carbiocial.sqlite"
     :make-pool? true})

(def file (slurp "c:/users/michael/development/carbiocial/solos/attributes-export.txt"))

(def parsed-csv (csv/parse-csv file :delimiter \tab))

(defn step-1-csv-to-seq 
  "read csv file and put into a clojure map, for easier handling"
  [csv]
  (for [[poly gid0 nome1 fonte2 nome-sig3 nome-com4] csv]
    {:poly poly
     :id (edn/read-string gid0)
     :nome1 nome1
     :fonte2 fonte2
     :nome-sig3 nome-sig3
     :nome-com4 nome-com4}))

(defn step-2-filter-water-regions [step]
  (filter (comp not (partial = "water") :nome1) step))

(defn step-3-seq-to-parse-poly 
  "take step 1 output and transform into a list of regions with attached polygon (list of polygon points)"
  [step]
  (for [{poly-str :poly 
         id :id 
         :as m} step]
    (let [p (subs poly-str 7)
          p2 (edn/read-string p)
          p3 (first p2)
          p4 (map-indexed (fn [i [lng lat]] 
                            {:region_id id 
                             :order_no i 
                             :lat lat 
                             :lng lng})
                  (partition 2 p3))]
      (assoc m :poly p4))))

(defn apply-steps 
  "shortcut to apply the steps 1 and 2 and return an input sequence for the database or js file creation"
  []
  (-> parsed-csv 
      rest 
      step-1-csv-to-seq 
      step-2-filter-water-regions
      step-3-seq-to-parse-poly))

(defn insert-all 
  "insert a prepared input sequence (applying steps 1 and 2) into the referenced sqlite db"
  [iseq]
  (doseq [region iseq]
    (apply j/insert! sqlite-db :soil_map_polygons (:poly region))
    (println "region id: " (:id region))))


(defn create-js 
  "creates javascript file to use in polygons.html to create output (to copy by hand to a file) for the mapping profile to region polygon id"
  [iseq]
  (let [helper "function LL(lat, lng){ return new google.maps.LatLng(lat,lng); }"
        
        regions (map (fn [region] 
                       (let [poly (map (fn [coord]
                                         (str "LL(" (:lat coord) "," (:lng coord) ")")) 
                                       (:poly region))
                             poly-str (clojure.string/join "," poly)]
                         (str "{id: " (:id region) ","
                              "poly: [" poly-str "]}")))
                     iseq)
        regions-str (str "var regions = [" (clojure.string/join "," regions) "];") 
                        
        query (j/query sqlite-db (s/select [:lat_times_10000 :lng_times_10000] :soil_data (s/where {:horizon_id 1})))
        
        profiles (map (fn [{lat10000 :lat_times_10000 lng10000 :lng_times_10000}]
                        (str "{lat10000: " lat10000 ", lng10000: " lng10000 
                             ", coord: LL(" (double (/ lat10000 10000)) "," (double (/ lng10000 10000)) ")}"))
                       query)
        
        profiles-str (str "var soilProfiles = [" (clojure.string/join "," profiles) "];")]
    
    (spit "regions-and-profiles.js" (str helper \newline \newline 
                                         regions-str \newline \newline 
                                         profiles-str))))

(defn profile-regions-mapping-to-insert-seq 
  "transform file input into input seq for db import"
  [& [rid-name lat-name lng-name]]
  (let [list-str (str "[" (slurp "profile-2-region-id.txt") "]")
        l (edn/read-string list-str)]
    (->> l
         (map (fn [{rid :region-id lat :lat10000 lng :lng10000}]
                {(or rid-name :region_id) rid 
                 (or lat-name :profile_lat_times_10000) lat
                 (or lng-name :profile_lng_times_10000) lng})
              ,,,)
         rest)))

(defn region-id-to-no-of-points [region+polygons-iseq]
  (reduce (fn [m region]
            (assoc m (:id region) {:rid (:id region) :count (count (:poly region))}))
          region+polygons-iseq))

(defn get-duplicates [profile-2-region-iseq]
  (let [all (reduce (fn [m {rid :region_id 
                            lat :profile_lat_times_10000
                            lng :profile_lng_times_10000}]
                     (let [ll {:lat lat :lng lng}
                           rs (get m ll [])]
                       (assoc m ll (conj rs rid))))
                   {} profile-2-region-iseq)
        
        ds (filter (fn [[k v]] (> (count v) 1)) all)
        
        ;_ (println ds)
                
        rid-2-ps (region-id-to-no-of-points (apply-steps))
        
        ;_ (println (take 10 rid-2-ps))
        
        ds* (into {} 
                  (map (fn [[ll rids]] 
                         (->> (map rid-2-ps rids)
                              (sort-by :count ,,,)
                              first
                              (vector ll ,,,)))
                       ds))
                
        ;_ (map println ds*)
        ]
    ds*))

(defn filter-out-duplicates 
  "filter out duplicates found in input seq"
  [profile-2-region-iseq]
  (let [ds (get-duplicates profile-2-region-iseq)]
    (filter (fn [{rid :region_id 
                  lat :profile_lat_times_10000
                  lng :profile_lng_times_10000}]
              (let [ll {:lat lat :lng lng}
                    {rid* :rid} (get ds ll)]
                (or (not rid*) (= rid* rid))))
            profile-2-region-iseq)))


(defn insert-profile-2-region-mapping [iseq]
  "insert into database"
  (apply j/insert! sqlite-db :soil_map iseq))


