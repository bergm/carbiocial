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

(defn step-1-csv-to-seq [csv]
  (for [[poly gid0 nome1 fonte2 nome-sig3 nome-com4] csv]
    {:poly poly
     :id (edn/read-string gid0)
     :nome1 nome1
     :fonte2 fonte2
     :nome-sig3 nome-sig3
     :nome-com4 nome-com4}))

(defn step-2-seq-to-parse-poly [step1]
  (for [{poly-str :poly 
         id :id 
         :as m} step1]
    (let [p (subs poly-str 7)
          p2 (edn/read-string p)
          p3 (first p2)
          p4 (map-indexed (fn [i [lat lng]] 
                            {:soil_map_id id 
                             :order_no i 
                             :lat lat 
                             :long lng})
                  (partition 2 p3))]
      (assoc m :poly p4))))

(def isc (atom 0))

(defn insert-all [iseq]
  (doseq [region iseq]
    (apply j/insert! sqlite-db :soil_map_polygons (:poly region))
    (println "region id: " (:id region))))
