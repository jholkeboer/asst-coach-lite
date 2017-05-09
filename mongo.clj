(ns asst-coach.mongo
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            monger.joda-time
            [clj-time.core :as time]
            [asst-coach.constants :refer [mongo-indexes]]
            [asst-coach.helpers :refer [make-games-url make-date-iso iso-string-to-date]])
  (:import (org.joda.time DateTime))
  (:refer-clojure :exclude [sort find]))

(def conn (mg/connect))
(def db (mg/get-db conn "nba"))

(defn ensure-indices []
  (doseq [[coll index] mongo-indexes]
    (if (= (type index) java.lang.String)
      (mc/ensure-index db coll {index -1} {:unique true})
      (let [index-map (reduce merge (map #(array-map % -1) index))]
        (mc/ensure-index db coll index-map {:unique true})))))

(defn get-newest [collection field]
  "Gets newest element of collection when sorted by field descending"
  (let [newest (-> (with-collection db collection
                    (find {})
                    (sort (array-map field -1))
                    (limit 1))
                  first
                  (get field))]
    (if (= (type newest) java.lang.String)
      (iso-string-to-date newest)
      newest)))

(defn upsert-doc [input]
  "Inserts mongo doc while respecting indexes, either string or vector"
  (let [{:keys [coll doc index]} input]
    (if-not index
      (mc/insert db coll (merge doc {:updated_at (DateTime.)}))
      (let [doc (merge doc {:updated_at (DateTime.)})
            index-type (type index)
            index-constraint  (if (= index-type java.lang.String)
                                (hash-map index (get doc index))
                                (reduce merge (map
                                                (fn [key] {key (get doc key)})
                                                index)))
            doc-without-index (if (= index-type java.lang.String)
                                (dissoc doc index)
                                (apply dissoc doc index))]
        (try
          (mc/update db coll index-constraint doc {:upsert true})
          (catch Exception e
            (mc/update db coll index-constraint {$set doc-without-index})))))))

(defn write-prediction [game-id predicted-spread]
  (mc/update db "spreads" {:game_id game-id} {$set {:predicted_spread predicted-spread}}))

(defn get-all-game-ids [initial?]
  "Get from mongo all game ids"
  (if initial?
    (mc/find db "game_list" {:date {$lte (make-date-iso (time/now))}} {:id 1 :date 1})
    (let [newest-game-date (get-newest "game_list" :date)]
      (mc/find db "game_list"
        {:date {$gte newest-game-date
                $lte (make-date-iso (time/now))}}
        {:id 1 :date 1}))))

(defn get-all-player-ids [initial?]
  (map str
    (if initial?
      (mc/distinct db "all_players" "PERSON_ID")
      (mc/distinct db "player_info" "PERSON_ID"))))

(defn remove-nils [m]
  (into {}
    (map (fn []))))

(defn get-feature-sets [until-date]
  "Gets games less than until-date e.g. 20071014"
  (let [features (mc/find-maps db "learning_machine" {})
        spreads (mc/find-maps db "spreads" {:date {$lt until-date} :actual_spread {$exists true}})]
      (remove nil?
        (into []
          (map (fn [[k v]]
                  (apply merge v))
            (group-by :game_id (concat features spreads)))))))

(defn get-actual-spreads []
  (let [today (make-date-iso (time/now))]
    (into {}
      (map (fn [x]
              {(:game_id x) (:actual_spread x)})
        (mc/find-maps db "spreads" {})))))
