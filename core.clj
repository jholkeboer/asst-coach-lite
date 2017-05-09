(ns asst-coach.core
  (:gen-class)
  (:require [asst-coach.api :as api :refer [refresh-batch]]
            [asst-coach.mongo :as mongo]
            [asst-coach.constants :refer [mongo-indexes]]
            [clj-time.core :as time]
            [asst-coach.helpers :refer [make-date-iso iso-string-to-date]]
            [clojure.core.async :as async :refer [>! <! >!! <!! put! go chan buffer close! thread alts! alts!! timeout]]))

(defn refresh-gamelist [initial?]
  "Get fresh game lists from server, or get all of them if initial is true"
  (let [latest-game-day (if-not initial? (mongo/get-newest "game_list" :date))
        new-games (api/get-gamelists latest-game-day)]
    (doseq [gamelist new-games]
      (doseq [g (get gamelist "game")]
        (if (get g "id")
          (mongo/upsert-doc {:coll "game_list" :doc g :index (get mongo-indexes "game_list")})
          (println "no game id"))))))

(defn -main
  "Call me Assistant Coach!"
  [& args]

  (let [initial? (contains? (set args) "-initial")]

    (if initial? (mongo/ensure-indices))

    (refresh-gamelist initial?)

    (let [job-chan (go
                      (<! (refresh-batch "league_wide" initial?))
                      (<! (refresh-batch "bs_summary" initial?))
                      (<! (refresh-batch "player_usage" initial?))
                      (<! (refresh-batch "pbp" initial?))
                      (<! (refresh-batch "box_score" initial?))
                      (<! (refresh-batch "player_info" initial?)))]


      (<!! job-chan)

      (Thread/sleep 20000))))
