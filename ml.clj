(ns asst-coach.ml
  (:require [asst-coach.mongo :as mongo]
            [asst-coach.features :refer :all]
            [asst-coach.constants :refer [mongo-indexes]]
            [lambda-ml.regression :refer [make-linear-regression
                                          regression-fit
                                          regression-predict]]))

(defn features-for-game [game-data]
  "Takes output of read-docs-for-game"
  (let [{:keys [game-id home-id visitor-id line-scores game-date]} game-data]
    {:game_id               game-id
     :game_date             game-date
     :ppg_diff              (get-ppg-diff game-data)
     :recent_games_diff     (- (games-played-recently home-id game-date line-scores)
                               (games-played-recently visitor-id game-date line-scores))}))

(defn compute-games []
  "Computes features for all games"
  (let [games         (mongo/get-all-game-ids true)
        lm-index      (get mongo-indexes "learning_machine")
        spread-index  (get mongo-indexes "spreads")]
    (doseq [g games]
      (let [game-id   (get g "id")
            game-date (get g "date")]
        (println "Computing game " game-id " " game-date " ...")
        (try
          (let [game-data     (read-docs-for-game game-id)
                game-features (features-for-game game-data)
                game-spread   (get-game-spread game-id)]
            (mongo/upsert-doc {:coll "learning_machine"
                               :doc game-features
                               :index lm-index})
            (mongo/upsert-doc {:coll "spreads"
                               :doc {:game_id game-id :actual_spread game-spread :date game-date}
                               :index spread-index}))
          (catch Exception e
            (do
              (println "Error on game " game-id)
              (println e)
              (System/exit 0))))))))

(defn read-regression-data []
  (let [games           (mongo/get-feature-sets)
        actual-spreads  (mongo/get-actual-spreads)]
    (remove nil?
      (into {}
        (for [g games]
          (let [game-id (:game_id g)]
            (get actual-spreads game-id)
            (if-let [spread (get actual-spreads game-id)]
              (let [feature-list (-> g
                                  (dissoc :updated_at :_id :game_id :game_date)
                                  (into (sorted-map))
                                  vals)]
                {game-id (conj (into [] feature-list) spread)}))))))))

(defn fit-model [rows]
  (let [alpha 0.01
        lambda 0
        iters 5000]
    (-> (make-linear-regression alpha lambda iters)
        (regression-fit rows))))

(defn make-predictions []
  (mongo/ensure-indices)
  (try
    (let [data (read-regression-data)
          fit  (fit-model (vals data))]
      (let [predictions (regression-predict fit (map butlast (vals data)))]
        (doall (map
                  (fn [game-id predicted-spread]
                    (mongo/write-prediction game-id predicted-spread))
                  (keys data) predictions))))
    (catch Exception e
      (println e))))

#_(def data (read-regression-data))
#_(def fit
    (let [alpha 0.01
          lambda 0
          iters 5000]
      (-> (make-linear-regression alpha lambda iters)
          (regression-fit (vals data)))))
#_(def predictions (regression-predict fit (map butlast (vals data))))
#_(doall (map
            (fn [game-id predicted-spread]
              ())
            (keys data)
            predicitons))
