(ns dvliman.leads
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.walk :as walk]))

(def base-path "/Users/dv/Desktop/")

(def opportunity-fields
  [:Name
   :Account_Phone_Primary__c
   :VIN__c
   :Guaranteed_Price__c
   :Quoted_Sale_Price__c
   :Final_Guaranteed_Price__c
   :Final_Guaranteed_Price_2__c
   :KBB_Price_Estimate__c
   :Make__c :Year__c :StageName
   :Region_Name__c
   :LeadSource
   :Vehicle_ID__c])

(def vehicle-fields [:Mileage__c])

(defn make-row [m]
  ((juxt :Name :Mileage__c :Account_Phone_Primary__c :VIN__c :Guaranteed_Price__c :Quoted_Sale_Price__c
         :Final_Guaranteed_Price__c
         :Final_Guaranteed_Price_2__c
         :KBB_Price_Estimate__c
         :Make__c
         :Year__c
         :StageName
         :Region_Name__c
         :LeadSource
         :last_modified_ts) m))

(defn to-csv [entries]
  (let [headers ["name" "mileage" "phone_number" "vin" "guaranteed_price" "quoted_sale_price"
                 "final_guaranteed_price" "final_guaranteed_price2" "kbb_estimate"
                 "make" "year" "stage" "region" "lead_source" "last_modified_ts"]
        rows (map make-row entries)]
    (apply conj [] headers rows)))

(defn process [suffix date]
  (let [source-file (str base-path suffix ".json")

        entries (->> (slurp source-file)
                     json/read-str
                     walk/keywordize-keys
                     (filter (comp #(= % "Los Angeles") :Region_Name__c :data))
                     (map #(merge (-> % :data    (select-keys opportunity-fields))
                                  (-> % :vehicle (select-keys vehicle-fields)))))

        with-date (fn [entries] (map #(assoc % :last_modified_ts date) entries))

        quotes (with-date (filter (comp #(= % "Quote and Schedule") :StageName) entries))
        losts  (with-date (filter (comp #(= % "Lost")               :StageName) entries))]

    (with-open [writer (io/writer (str base-path suffix "-quote.csv"))]
      (csv/write-csv writer (to-csv quotes)))

    (with-open [writer (io/writer (str base-path suffix "-lost.csv"))]
      (csv/write-csv writer (to-csv losts)))))

(process "june-23" "2022-06-23")
