# COVID19-dataprep-SAC

Simple Python script to read COVID-19 data and some related information from various sources and prepare data for downstream use.

**_main file: COVIDdataprep.py_** 

(Missing in this repository is the json file that contains the authorization for the GDRive / GSheets API)

We are using here normalized and cleansed data from
 https://github.com/datasets/covid-19
derived from the original
 https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
and for US on county-level from:
 https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv
and for DE for laender-level from:
 https://github.com/jgehrcke/covid-19-germany-gae


also good reads:
 https://www.kaggle.com/c/covid19-global-forecasting-week-3/notebooks  #KAGGLE!, webscraper 
 https://github.com/pomber/covid19  # VAST resources
 https://medium.com/analytics-vidhya/mapping-the-spread-of-coronavirus-covid-19-d7830c4282e
 https://blogs.sap.com/2020/03/11/quickly-load-covid-19-data-with-hana_ml-and-see-with-dbeaver/
 https://developers.google.com/public-data/docs/canonical/countries_csv
 https://www.r-bloggers.com/making-of-a-free-api-for-covid-19-data/   -> for later, do sth with GCR, Docker,...


### PREREQUISITES:
### a file continentslatlon.csv which contains master data for continents
### a file countryshortlong.csv, which contains a translation of 2letter to 3letter country codes
### a file countriescontlatlon.csv which contains master data for the countries that are coming in the covid-19 dataset
### a file GlobalHealthExpenditures2016.csv which contains some additional data from Health care
### a file googlecoviddata.json which contains credentials for GSheets and GCP API
