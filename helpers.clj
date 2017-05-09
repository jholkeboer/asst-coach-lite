(ns asst-coach.helpers
  (require  [clj-http.client :as client]
            [clj-time.core :as time]
            [cheshire.core :as json]))

; General helper functions

(def http-options {:headers {"referrer" "http://stats.nba.com/scores/"
                             "User-Agent" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"}
                   :socket-timeout 90000
                   :conn-timeout 90000
                   :follow-redirects true
                   :insecure? true})

(defn http-get
    ([url]
     (client/get url http-options))
    ([url params]
     (client/get url
      (merge http-options
        (if params {:query-params params})))))

#_(http-get "http://stats.nba.com/stats/playbyplayv2?GameId=0021601203&StartPeriod=0&EndPeriod=0")

(defn zero-pad [num]
  (let [s (str num)]
    (condp = (count s)
      4 s
      2 s
      1 (str "0" s))))

(defn make-date-iso [date]
  (reduce str
    (map zero-pad
      [(time/year date) (time/month date) (time/day date)])))

(defn iso-string-to-date [s]
  "Turns '20140302' into a date object"
  (try
    (let [year  (->> s (take 4) (apply str) Integer.)
          month (->> s (drop 4) (take 2) (apply str) (Integer.))
          day   (->> s (take-last 2) (apply str) (Integer.))]
      (time/date-time year month day))
    (catch java.lang.NumberFormatException e
      nil)))

(defn hyphen-pad-date [d]
  "Turns '20170227' into 2017-02-27"
  (str
    (apply str (take 4 d)) "-"
    (apply str (take 2 (drop 4 d))) "-"
    (apply str (take-last 2 d))))

(defn get-date-range [start-date]
  (let [start-date            (if-not start-date (time/date-time 2007 07 01) start-date)
        current-year          (time/year (time/now))
        before-july?          (< (time/month (time/now)) 7)
        end-year              (if before-july? current-year (inc current-year))
        end-date              (time/date-time end-year 6 30)
        interval              (time/interval start-date end-date)
        days                  (time/in-days interval)]
    (into []
      (map
        (fn [x]
          (time/plus start-date (time/days x)))
        (range 0 days)))))

(defn make-games-url [date]
  (str
    "http://data.nba.com/5s/json/cms/noseason/scoreboard/"
    (make-date-iso date)
    "/games.json"))


(defn parse-row-sets [headers row-set]
  "Returns one doc for each row in the row-set"
  (into []
    (map
      (fn [row]
        (into {} (map array-map headers row)))
      row-set)))

(defn parse-officials [headers row-set gameid]
  "Injects game-id into docs"
  (into []
    (map
      (fn [row]
        (merge {"GAME_ID" gameid}
          (reduce merge
            (map array-map headers row))))
      row-set)))

(defn single-row [headers row-set]
  "Extracts map of headers to single row"
  (into {}
    (map array-map headers (first row-set))))

(defn format-box-score [result-set]
  (if-not result-set nil
    (into []
      (map
        (fn [result]
          (let [headers     (get result "headers")
                row-set     (get result "rowSet")
                result-name (get result "name")
                docs        (parse-row-sets headers row-set)]
            {:name result-name :docs docs}))
        result-set))))

(defn get-first-with-key [coll key]
  (first (filter #(= %1 key) coll)))
