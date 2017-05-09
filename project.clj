(defproject asst-coach "1.0.0-SNAPSHOT"
  :description "I gotta make the NBA."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "0.0-2843"]
                 [proto-repl "0.3.1"]
                 [cheshire "5.7.0"]
                 [clj-time "0.13.0"]
                 [clj-http "3.4.1"]
                 [ring "1.4.0"]
                 [compojure "1.5.0"]
                 [hiccup "1.0.5"]
                 [clj-stacktrace "0.2.8"]
                 [com.novemberain/monger "3.0.2"]
                 [org.clojure/core.async "0.2.395"]
                 [lambda-ml "0.1.0"]
                 [uncomplicate/bayadera "0.1.0-SNAPSHOT"]]
  :target-path "target/%s"
  :source-paths ["src/clj"]
  :plugins [[lein-localrepo "0.5.3"]
            [lein-cljsbuild "1.0.4"]]
  :cljsbuild {:builds
              [{:id "app"
                :source-paths ["src/cljs"]
                :compiler {:output-to "resources/public/js/app.js"
                           :output-dir "resources/public/js/out"
                           :source-map true
                           :optimizations :none
                           :asset-path "/static/js/out"
                           :main "asst-coach.core"
                           :pretty-print true}}]}
  :main asst-coach.core
  :profiles {:uberjar {:aot :all}})
