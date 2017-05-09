(ns asst-coach.gpu
  (:require [asst-coach.mongo :as mongo]
            [asst-coach.features :refer :all]
            [quil.core :as q]
            [quil.applet :as qa]
            [clj-stacktrace.core :refer :all]
            [quil.middlewares.pause-on-error :refer [pause-on-error]]
            [uncomplicate.commons.core :refer [with-release let-release wrap-float]]
            [uncomplicate.fluokitten.core :refer [op fmap]]
            [uncomplicate.neanderthal
             [core :refer [dim]]
             [real :refer [entry entry!]]
             [math :refer [sqrt]]
             [native :refer [sv sge]]]
            [uncomplicate.bayadera
             [protocols :as p]
             [core :refer :all]
             [util :refer [bin-mapper hdi]]
             [opencl :refer [with-default-bayadera]]
             [mcmc :refer [mix! burn-in! pow-n acc-rate! run-sampler!]]]
            [uncomplicate.bayadera.opencl.models
             :refer [source-library cl-distribution-model cl-likelihood-model]]
            [uncomplicate.bayadera.toolbox
             [processing :refer :all]
             [plots :refer [render-sample render-histogram]]]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(def all-data (atom {}))
(def state (atom nil))

(defmacro conj-features [res row features]
  "Macro to generate additional conj calls depending on number of features"
  (let [conj-calls (map
                    (fn [x] `(conj! (double (get ~row ~x))))
                    (eval features))]
    `(-> ~res
       ~@conj-calls)))

(defn get-training-set []
  "Reads data from learning_machine in mongo. Puts it in bayadera format"
  (let [feature-data (mongo/get-feature-sets "20170114")]
    (loop [c 0 data feature-data res (transient [])]
      (if data
        (let [row (assoc (first data) :fake_param (nth [1 2 3 3 4 5] (int (rand 5))))]
          (if (not-any? nil? ((apply juxt feature-list) row))
            (recur (inc c) (next data)
              (conj-features res row feature-list))
            (recur c (next (next data)) res)))
        (sv (op [c] (persistent! res)))))))

(def params (sv (get-training-set)))

(defn get-size-params [feature-list]
  "Length of feature list is all params plus output"
  (let [n (count feature-list)]
    {:params-size (+ 3 (* 2 n))
     :dimension (+ n 2)}))

(def mlr-prior
  (let [{:keys [params-size dimension]} (get-size-params feature-list)]
    (cl-distribution-model [(:gaussian source-library)
                            (:uniform source-library)
                            (:exponential source-library)
                            (:t source-library)
                            (slurp (io/resource "multiple-linear-regression.h"))]
                           :name "mlr" :mcmc-logpdf "mlr_mcmc_logpdf" :params-size params-size :dimension dimension)))

(defn get-kernel-consts [feature-list]
  "e.g. for 3 x params:
   const REAL b1 = x[3];
   const REAL b2 = x[4];
   const REAL b3 = x[5];"
    (apply str
      (drop-last 5
        (apply str
          (map
            (fn [x]
              (str "const REAL b" x " = x[" (+ x 2) "];\n    "))
            (range 1 (count feature-list)))))))

(defn get-kernel-xvalues [feature-list]
  "e.g. if 3 x params:
   b1 * params[i+1] + b2 * params[i+2] + b3 * params[i+3]"
  (apply str
    (drop-last 3
      (apply str
        (map
          (fn [x] (str "b" x " * params[i+" x "]" " + "))
          (range 1 (count feature-list)))))))

(defn expand-kernel-params [feature-list]
  "mlr.h is kernel code with %s in places we want to adjust"
  (let [n     (count feature-list)
        kernel (slurp (io/resource "mlr.h"))]
    (apply str
      (remove #(= % \return)
        (apply (partial format kernel)
          [(get-kernel-consts feature-list)
           n
           (get-kernel-xvalues feature-list)
           n])))))

(defn rhlr-likelihood [n]
  (let [kernel  (expand-kernel-params feature-list)]
    ; (cl-likelihood-model (slurp (io/resource "multiple-linear-regression.h"))
    (cl-likelihood-model kernel
                         :name "mlr" :params-size n)))

#_(reset! all-data (analysis))
(defn analysis []
  (with-default-bayadera
    (with-release [prior (distribution mlr-prior)
                   prior-dist (prior (sv [10 0.001 10 0 10 0 5 0 5 0 5]))
                   post (posterior "mlr" (rhlr-likelihood (dim params)) prior-dist)
                   post-dist (post params)
                   post-sampler (sampler post-dist {:limits (sge 2 5 [-10 10 0.001 10 0 10 -5 5 -5 5 -5 5])})]
      (println (time (mix! post-sampler {:cooling-schedule (pow-n 2)})))
      (println (time (do (burn-in! post-sampler 1000) (acc-rate! post-sampler))))
      (println (time (run-sampler! post-sampler 64)))
      (time (histogram! post-sampler 1000)))))

(defn get-pdf-x-values [[lower-bound upper-bound] slices]
  "Limits is vector of lower and upper bound.
   Gives a seq of x values where number of entries = slices"
   (let [width (- upper-bound lower-bound)]
    (range lower-bound upper-bound (/ width slices))))

(defn closest-param-match [param x-seq]
  "Returns the index of x-seq which holds the value closest to param"
  (reduce
    (fn [sum n]
      (if (< n param)
        (inc sum)
        (reduced sum)))
    0 x-seq))

(defn get-max-x-value-index [pdf]
  "Gets index of max value. Useful for b0"
  (let [m (apply max pdf)]
    (reduce
      (fn [prev x]
        (println x)
        (if (< x m)
          (inc prev)
          (reduced (inc prev))))
      -1 pdf)))

(defn get-b0-effect [limits pdf]
  "Gets x-value from b0 with highest probability"
  (let [x-seq (get-pdf-x-values limits (count pdf))
        x-index (get-max-x-value-index pdf)]
    (nth x-seq x-index)))

#_(predict [2 1 0])
(defn predict [params]
  "Takes game params from mongo as a vector and extracts prediction.
   First two params are nu and sigma, they are ignored.
   Third one is the intercept b0.
   The rest are params from the model.
   The input params start at b1 and goes from there.
   Output is a vector of the cumulative probabilities."
  (let [{:keys [limits pdf bin-ranks]} @all-data
        limits (into [] limits)
        pdf    (into [] pdf)
        bin-ranks (into [] bin-ranks)]
    (flatten
       [(get-b0-effect (nth limits 2) (nth pdf 2))
        (into []
          (for [n (range 0 (count params))]
            (let [nth-pdf  (nth pdf (+ n 3))
                  nth-limits (nth limits (+ n 3))
                  lower-bound (first nth-limits)
                  upper-bound (second nth-limits)
                  nth-param (nth params n)]
              (if-not (and (>= nth-param lower-bound)
                           (<= nth-param upper-bound))
                (throw (Exception. (str n "th parameter was outside its limits.  "
                                    nth-param " is not between " lower-bound " and " upper-bound)))
                (let [x-seq (get-pdf-x-values nth-limits (count nth-pdf))
                      x-index (closest-param-match nth-param x-seq)]
                  (nth nth-pdf x-index))))))])))

(defn setup []
  (reset! state
          {:data @all-data
           :nu (plot2d (qa/current-applet) {:width 500 :height 500})
           :sigma (plot2d (qa/current-applet) {:width 500 :height 500})
           :b0 (plot2d (qa/current-applet) {:width 500 :height 500})
           :b1 (plot2d (qa/current-applet) {:width 500 :height 500})
           :b2 (plot2d (qa/current-applet) {:width 500 :height 500})
           :b3 (plot2d (qa/current-applet) {:width 500 :height 500})}))

(defn draw []
  (when-not (= @all-data (:data @state))
    (swap! state assoc :data @all-data)
    (let [data @all-data]
      (q/background 0)
      (q/image (show (render-histogram (:nu @state) data 0)) 0 0)
      (q/image (show (render-histogram (:sigma @state) data 1)) 520 0)
      (q/image (show (render-histogram (:b3 @state) data 5)) 1040 0)
      (q/image (show (render-histogram (:b0 @state) data 2)) 0 520)
      (q/image (show (render-histogram (:b1 @state) data 3)) 520 520)
      (q/image (show (render-histogram (:b2 @state) data 4)) 1040 520))))

(defn display-sketch []
  (q/defsketch diagrams
    :renderer :p2d
    :size [2000 1200]
    :display 2
    :setup setup
    :draw draw
    :middleware [pause-on-error]))
