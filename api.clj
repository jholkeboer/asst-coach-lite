(ns asst-coach.api
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clj-time.core :as time]
            [clj-time.coerce :as coerce]
            [clojure.pprint :refer [pprint]]
            [asst-coach.helpers :refer :all]
            [asst-coach.constants :refer :all]
            [asst-coach.mongo :refer [upsert-doc get-all-game-ids get-all-player-ids get-newest]]
            [clojure.core.async :as async :refer [>! <! >!! <!! put! go chan buffer close! thread alts! alts!! timeout]])
  (:import (org.joda.time DateTime)))

(def requests-made (atom 0))
(def requests-responded (atom 0))
(def start-time (atom nil))
(def SLEEP-MULTIPLIER 1)
(def SLEEP-INTERVAL 1010)

(defn get-sleep-time []
  (* SLEEP-INTERVAL SLEEP-MULTIPLIER))

(defn get-elapsed-time []
  (time/in-minutes
    (time/interval
      (coerce/from-long @start-time)
      (DateTime.))))

(defn inc-requests-made [message]
  (swap! requests-made inc)
  (println (str "requesting  " message))
  (println (str "Fulfilled " @requests-responded " out of " @requests-made " requests  "
              (format "%.2f" (* 100 (float (/ @requests-responded @requests-made)))) "%"))
  (println (str "Time elapsed:  " (get-elapsed-time) " minutes")))

(defn inc-requests-responded [message]
  (swap! requests-responded inc)
  (println (str "got " message)))

;;;;;;;;;;;;;;;;;;;;
;; GAME LISTINGS
;;;;;;;;;;;;;;;;;;;;
(defn get-gamelists [start-date]
  "Nothing core.async going on here currently. Called from core.
   Is pmap better than go blocks here? Probably not."
  (remove nil?
    (into []
      (pmap
        (fn [date]
          (println (str "Getting games for " date))
          (let [response (try
                            (http-get (make-games-url date))
                            (catch Exception e (println (.getMessage e))))]
            (if response
              (-> (http-get (make-games-url date))
                :body
                json/parse-string
                (get "sports_content")
                (get "games")))))
        (get-date-range start-date)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; BASIC LEAGUE-WIDE INFO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn basic-result-set-call [url]
  (go (->
        (http-get url)
        (get :body)
        json/parse-string
        (get "resultSets"))))

(defn get-league-wide-docs [c initial?]
  (println "Refreshing league wide info...")
  (go
    (let [team-years  (first (<! (basic-result-set-call team-years-url)))
          all-players (first (<! (basic-result-set-call all-players-url)))]
      (doseq [result-set [team-years all-players]]
        (let [coll    (case (get result-set "name")
                        "TeamYears"         "team_years"
                        "CommonAllPlayers"  "all_players")
              headers (get result-set "headers")
              row-set (get result-set "rowSet")]
          (println (str "Writing " coll " ..."))
          (doseq [doc (parse-row-sets headers row-set)]
            (let [index (get mongo-indexes coll)]
              (>! c {:coll coll :doc doc :index index})))))
      (close! c))))

;;;;;;;;;;;;;;;;;;;;
;; PLAY BY PLAY
;;;;;;;;;;;;;;;;;;;;
(defn- pbp-call [gameid]
  "Returns async channel for http call to play by play"
  (go
    (let [params (assoc pbp-params "GameID" gameid)
          result  (->
                    (http-get pbp-url params)
                    (get :body)
                    json/parse-string
                    (get "resultSets")
                    first)]
      {:headers (get result "headers")
       :row-set (get result "rowSet")})))

(defn get-pbp-docs [c initial?]
  "Get pbp docs asynchronously and write them to channel c."
  (go
    (let [games       (get-all-game-ids initial?)
          ;; FIXME updated_at is not necessarily newest
          pbp-index       (get mongo-indexes "pbp")
          newest-pbp  (get (get-newest "pbp" :updated_at) "GAME_ID")]
      (doseq [g (take-while (fn [g]
                              (let [game-date (iso-string-to-date (get g "date"))]
                                (if newest-pbp
                                  (time/after? game-date newest-pbp)
                                  true)))
                        games)]
        (let [game-id     (get g "id")
              game-date   (get g "date")]
          (inc-requests-made (str "playbyplay game " game-id " " game-date))
          (Thread/sleep (get-sleep-time))
          (go
            (let [{:keys [headers row-set]} (<! (pbp-call game-id))]
                (inc-requests-responded (str "playbyplay game " game-id " " game-date))
                (if (= 0 (count row-set))
                  (println "no rows"))
                (swap! requests-responded inc)
                (doseq [row row-set]
                  (let [doc (reduce merge (map array-map headers row))]
                    (put! c {:coll "pbp" :doc doc :index pbp-index})))))))
      (close! c))))

;;;;;;;;;;;;;;;;;;;;
;; BOX SCORE SUMMARY
;;;;;;;;;;;;;;;;;;;;
(defn resultSets-for-game [url params game-id]
  "TODO merge all the api call funcs into one"
  (go
    (try
      (->
        (http-get url (assoc params "GameID" game-id))
        (get :body)
        json/parse-string
        (get "resultSets"))
      (catch Exception e
        (println (str "Request error on " game-id))
        nil))))

(defn format-bs-summary-results [bs-results game-id]
  (if-not bs-results nil
    (reduce
      (fn [prev next]
        (let [headers (get next "headers")
              row-set (get next "rowSet")
              result-name (get next "name")]
          (merge prev
            (case result-name
              "GameSummary"     {:game-summary      (single-row headers row-set)}
              "Officials"       {:officials         (parse-officials headers row-set game-id)}
              "InactivePlayers" {:inactive-players  (parse-row-sets headers row-set)}
              "LineScore"       {:line-score        (parse-row-sets headers row-set)}
              "LastMeeting"     {:last-meeting      (single-row headers row-set)}
              "SeasonSeries"    {:season-series     (single-row headers row-set)}
              nil))))
      {} bs-results)))

(defn get-bs-summary-docs [c initial?]
  (go
    (let [games (get-all-game-ids initial?)
          bs-summary-index (get mongo-indexes "bs_summary")
          line-score-index (get mongo-indexes "line_score")
          officials-index  (get mongo-indexes "officials")]
      (doseq [g games]
        (let [game-id (get g "id")
              game-date (get g "date")]
          (inc-requests-made (str "bs summary  " game-id " " game-date))
          (Thread/sleep (get-sleep-time))
          (go
            (let [result-set (<! (resultSets-for-game bs-summary-url bs-summary-params game-id))
                  bs-docs    (format-bs-summary-results result-set game-id)]
                (inc-requests-responded (str "bs_summary game " game-id " " game-date))
                (if bs-docs
                  (do
                    (let [bs-summary-doc (merge
                                          (:game-summary bs-docs)
                                          {"INACTIVE_PLAYERS" (:inactive-players bs-docs)}
                                          {"LAST_MEETING"     (:last-meeting bs-docs)}
                                          {"SEASON_SERIES"    (:season-series bs-docs)})]
                      (go
                        (>! c {:coll "bs_summary" :doc bs-summary-doc :index bs-summary-index})
                        (doseq [l (:line-score bs-docs)]
                          (>! c {:coll "line_score" :doc l :index line-score-index}))
                        (doseq [o (:officials bs-docs)]
                          (>! c {:coll "officials"  :doc o :index officials-index}))))))))))
      (close! c))))

(defn get-box-score-docs [c initial?]
  (go
    (let [games (get-all-game-ids initial?)]
      (doseq [g games]
        (let [game-id   (get g "id")
              game-date (get g "date")]
          (Thread/sleep (get-sleep-time))
          (inc-requests-made (str "box score game " game-id " " game-date))
          (go
            (let [result-set        (<! (resultSets-for-game box-score-url box-score-params game-id))
                  parsed-results    (format-box-score result-set)]
              (inc-requests-responded (str "box score game " game-id " " game-date))
              (if parsed-results
                (go
                  (doseq [result parsed-results]
                    (let [coll (case (:name result)
                                  "PlayerStats"           "player_stats"
                                  "TeamStats"             "team_stats"
                                  "TeamStarterBenchStats" "team_starter_bench_stats")
                          index (get mongo-indexes coll)]
                      (doseq [d (:docs result)]
                        (>! c {:coll coll :doc d :index index}))))))))))
      (close! c))))

;;;;;;;;;;;;;;;;;;;;;;;
;; PLAYER DATA
;;;;;;;;;;;;;;;;;;;;;;;
(defn get-player-usage-docs [c initial?]
  "Needs initial handling"
  (go
    (let [games (get-all-game-ids initial?)
          player-usage-index (get mongo-indexes "player_usage")]
      (doseq [g games]
        (let [game-id   (get g "id")
              game-date (get g "date")]
          (inc-requests-made (str "player_usage game " game-id " " game-date))
          (Thread/sleep (get-sleep-time))
          (go
            (let [result-sets (<! (resultSets-for-game usage-url bs-summary-params game-id))]
                (inc-requests-responded (str "player_usage game " game-id " " game-date))
                (if result-sets
                  (do
                    (doseq [s result-sets]
                      (if (= (get s "name") "sqlPlayersUsage")
                        (let [headers (get s "headers")]
                          (doseq [r (get s "rowSet")]
                            (let [doc (reduce merge (map array-map headers r))]
                              (go (>! c {:coll "player_usage" :doc doc :index player-usage-index}))))))))))))))))

(defn player-info-call [url player-id params]
  (go
    (try
      (let [result-sets (->
                          (http-get url (assoc params "playerID" player-id))
                          (get :body)
                          json/parse-string
                          (get "resultSets"))]
        result-sets)
      (catch Exception e
        (println (str "Request error on player info " player-id))
        (pprint e)
        nil))))

(defn get-player-info-docs [c initial?]
  "Needs initial handling"
  (go
    (let [player-ids            (get-all-player-ids initial?)
          player-game-log-index (get mongo-indexes "player_game_log")
          player-info-index     (get mongo-indexes "player_info")]
      (doseq [player-id player-ids]
        (Thread/sleep (* 3 (get-sleep-time)))
        (inc-requests-made (str "player game log reg " player-id))
        (inc-requests-made (str "player game log playoff " player-id))
        (go
          (let [player-game-log-reg      (first (<! (player-info-call player-game-log-url player-id
                                                      (assoc player-log-params "SeasonType" "Regular Season"))))
                player-game-log-playoffs (first (<! (player-info-call player-game-log-url player-id
                                                      (assoc player-log-params "SeasonType" "Playoffs"))))
                player-info              (<! (player-info-call player-info-url player-id {}))]
              (inc-requests-responded (str "player game log regular " player-id))
              (inc-requests-responded (str "player game log playoff " player-id))
              (inc-requests-responded (str "player info " player-id))
              (if player-game-log-reg
                (let [headers  (get player-game-log-reg "headers")
                      row-set  (get player-game-log-reg "rowSet")
                      pgl-docs (parse-row-sets headers row-set)]
                  (doseq [doc pgl-docs]
                    (go (>! c {:coll "player_game_log" :doc doc :index player-game-log-index})))))
              (if player-game-log-playoffs
                (let [headers  (get player-game-log-playoffs "headers")
                      row-set  (get player-game-log-playoffs "rowSet")
                      pgl-docs (parse-row-sets headers row-set)]
                  (doseq [doc pgl-docs]
                    (go (>! c {:coll "player_game_log" :doc doc :index player-game-log-index})))))
              (if player-info
                (doseq [result-set player-info]
                  (let [result-name (get result-set "name")
                        headers (get result-set "headers")
                        row-set (get result-set "rowSet")
                        docs    (parse-row-sets headers row-set)]
                    (doseq [doc docs]
                      (let [coll (case result-name
                                    "CommonPlayerInfo" "player_info"
                                    "PlayerHeadlineStats" "player_headline_stats"
                                    nil)]
                        (if coll
                          (go (>! c {:coll "player_info" :doc doc :index player-info-index})))))))))))
      (close! c))))

;;;;;;;;;;;;;;;;;;;;
;; GENERALIZED API CALL
;;;;;;;;;;;;;;;;;;;;
; Map the function to the batch name here
(def retrieval-functions {"pbp"           get-pbp-docs
                          "bs_summary"    get-bs-summary-docs
                          "player_usage"  get-player-usage-docs
                          "player_info"   get-player-info-docs
                          "league_wide"   get-league-wide-docs
                          "box_score"     get-box-score-docs})

(defn refresh-batch [batch-type initial?]
  "Fetches fresh records for collection
   retrieval-function must accept a channel and a boolean for initial or not
   If initial is true, it should get games from beginning of time.
   TODO: add date ranges"
  (if-not @start-time
    (reset! start-time (System/currentTimeMillis)))
  (let [c (chan 1000000)
        ret-function (get retrieval-functions batch-type)
        result-chan  (ret-function c initial?)]
    (go
      (loop [next (<! c)]
        (if next
          (do
            (upsert-doc next)
            (recur (<! c))))))
    (go
      (<! result-chan))))
