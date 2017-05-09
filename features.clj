(ns asst-coach.features
  (:require [asst-coach.mongo :refer [db conn]]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.conversion :refer [from-db-object]]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [clojure.pprint :refer [pprint]]
            monger.joda-time
            [clj-time.core :as time]
            [asst-coach.helpers :refer [hyphen-pad-date
                                        iso-string-to-date
                                        make-date-iso]])
  (:import (org.joda.time DateTime))
  (:refer-clojure :exclude [sort find]))

;; In this feature list, the last item is the predictor
(def feature-list
  [:ppg_diff
   :recent_games_diff
   :fake_param
   :actual_spread])

(defn get-game-spread [game-id]
  "+/- score differential for home team"
  (let [game-doc    (mc/find-one-as-map db "bs_summary" {:GAME_ID game-id})
        home-id     (:HOME_TEAM_ID game-doc)
        visitor-id  (:VISITOR_TEAM_ID game-doc)]
    (reduce
      (fn [prev next]
        (if (:PTS next)
          (if (= (:TEAM_ID next) home-id)
            (+ prev (:PTS next))
            (- prev (:PTS next)))
          prev))
      0 (mc/find-maps db "line_score" {:GAME_ID game-id}))))

(defn get-ppg-diff [{:keys [game-id line-scores home-id visitor-id]}]
  "PPG differential for the last ? games"
  (let [home-games      (set (map :GAME_ID (filter #(= (Integer/parseInt home-id) (:TEAM_ID %)) line-scores)))
        visitor-games   (set (map :GAME_ID (filter #(= (Integer/parseInt visitor-id) (:TEAM_ID %)) line-scores)))
        home-ppg-diff   (reduce (fn [prev next]
                                  (let [points (:PTS next)]
                                    (if points
                                      {:diff (if (= (:TEAM_ID next) (Integer/parseInt home-id))
                                                (+ (:diff prev) points)
                                                (- (:diff prev) points))
                                       :count (inc (:count prev))}
                                      prev)))
                                {:count 0 :diff 0} (filter #(contains? home-games (:GAME_ID %)) line-scores))
        visitor-ppg-diff  (reduce (fn [prev next]
                                    (let [points (:PTS next)]
                                      (if points
                                        {:diff (if (= (:TEAM_ID next) (Integer/parseInt visitor-id))
                                                  (+ (:diff prev) points)
                                                  (- (:diff prev) points))
                                         :count (inc (:count prev))}
                                        prev)))
                                {:count 0 :diff 0} (filter #(contains? visitor-games (:GAME_ID %)) line-scores))]
    (try (float (/ (- (:diff home-ppg-diff) (:diff visitor-ppg-diff))
                   (+ (:count home-ppg-diff) (:count visitor-ppg-diff))))
         (catch ArithmeticException e 0))))

(defn games-played-recently [team-id game-date-iso line-scores]
  (let [game-date (iso-string-to-date game-date-iso)
        days-ago  (-> (time/minus game-date (time/days 10))
                    make-date-iso
                    hyphen-pad-date)]
    (count
      (filter #(> 0 (compare days-ago (:GAME_DATE_EST %)))
        (filter #(= (:TEAM_ID %) (Integer/parseInt team-id))
          line-scores)))))

(defn line-scores-for-game [game-ids]
  "Takes vector of game ids"
  (doall
    (into [] (mc/find-maps db "line_score" {:GAME_ID {$in game-ids}}))))

(defn get-prior-games [team-ids game-date]
  "Takes vector of home and visitor id as ints"
  (concat
    (into []
      (take 50
        (with-collection db "bs_summary"
          (find {$or [{:HOME_TEAM_ID (first team-ids)}
                      {:VISITOR_TEAM_ID (first team-ids)}]
                 :GAME_DATE_EST {$lt (hyphen-pad-date game-date)}})
          (sort {:GAME_DATE_EST -1}))))
    (into []
      (take 50
        (with-collection db "bs_summary"
          (find {$or [{:HOME_TEAM_ID (second team-ids)}
                      {:VISITOR_TEAM_ID (second team-ids)}]
                 :GAME_DATE_EST {$lt (hyphen-pad-date game-date)}})
          (sort {:GAME_DATE_EST -1}))))))

(defn read-docs-for-game [game-id]
  (let [game-list-doc     (mc/find-one-as-map db "game_list" {:id game-id})
        game-date         (:date game-list-doc)
        home-id           (get-in game-list-doc [:home :id])
        visitor-id        (get-in game-list-doc [:visitor :id])
        int-team-ids      (into [] (map #(Integer/parseInt %) [home-id visitor-id]))
        prior-games       (get-prior-games int-team-ids game-date)
        most-recent-game  (first prior-games)
        prior-game-ids    (doall (into [] (map :GAME_ID prior-games)))
        player-game-logs  (mc/find-maps db "player_game_log" {:Game_ID {$in prior-game-ids}})
        player-usage      (mc/find-maps db "player_usage" {:GAME_ID {$in prior-game-ids}})
        player-ids        (doall (into #{} (map :PLAYER_ID (filter #(= (:GAME_ID %) (:GAME_ID most-recent-game)) player-usage))))
        player-stats      (mc/find-maps db "player_stats" {:GAME_ID {$in prior-game-ids}})
        starter-bench     (mc/find-maps db "team_starter_bench_stats" {:GAME_ID {$in prior-game-ids}})
        pbp               (mc/find-maps db "pbp" {:GAME_ID {$in prior-game-ids}})]
    {:game-id               game-id
     :game-date             game-date
     :most-recent-game      most-recent-game
     :home-id               home-id
     :visitor-id            visitor-id
     :game-list-doc         game-list-doc
     :line-scores           (line-scores-for-game prior-game-ids)
     :prior-games           prior-games
     :prior-game-ids        prior-game-ids
     :player-ids            player-ids
     :player-game-logs      player-game-logs
     :player-usage          player-usage
     :player-stats          player-stats
     :starter-bench         starter-bench
     :pbp                   pbp}))
