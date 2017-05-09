# asst-coach-lite
Selections from my NBA betting code in Clojure - ETL and GPU-powered analysis

What's going on here?

- Scraping data from NBA.com (api.clj, core.clj)
- Storing data in MongoDB (mongo.clj)
- Running Bayesian regression on the GPU using OpenCL and the Bayadera library (gpu.clj)
