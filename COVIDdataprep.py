# we are using here normalized and cleansed data from
# https://github.com/datasets/covid-19
# derived from the original
# https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
# and for US on county-level from:
# https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv
# and for DE for laender-level from:
# https://github.com/jgehrcke/covid-19-germany-gae
#df_confd = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
#df_death = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
#df_recvd = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')


#also good reads:
# https://www.kaggle.com/c/covid19-global-forecasting-week-3/notebooks  #KAGGLE!, webscraper 
# https://github.com/pomber/covid19  # VAST resources
# https://medium.com/analytics-vidhya/mapping-the-spread-of-coronavirus-covid-19-d7830c4282e
# https://blogs.sap.com/2020/03/11/quickly-load-covid-19-data-with-hana_ml-and-see-with-dbeaver/
# https://developers.google.com/public-data/docs/canonical/countries_csv
# https://www.r-bloggers.com/making-of-a-free-api-for-covid-19-data/   -> for later, do sth with GCR, Docker,...


### PREREQUISITES:
### a file continentslatlon.csv which contains master data for continents
### a file countryshortlong.csv, which contains a translation of 2letter to 3letter country codes
### a file countriescontlatlon.csv which contains master data for the countries that are coming in the covid-19 dataset
### a file GlobalHealthExpenditures2016.csv which contains some additional data from Health care
### a file googlecoviddata.json which contains credentials for GSheets and GCP API

import pandas, csv, json
import urllib.request
import datetime
### technically, the other imports should also all be here and not in the functions defs, being lazy for now

def load_continent_latlon():
    load_saved = 'continentslatlon.csv'
    ContinentlatlonDict = dict()
    try:
        continentslatlon_csv = csv.reader(open(load_saved, 'r'))
        next(continentslatlon_csv)  #skip first row with headers
        for row in continentslatlon_csv:
            k, v1, v2  = row
            ContinentlatlonDict[k] = (float(v1),float(v2))
        return ContinentlatlonDict
    except:
        pass

def load_countries_latlon():
    load_saved = 'countriescontlatlon.csv'
    CountlatlonDict = dict()
    try:
        continentslatlon_csv = csv.reader(open(load_saved, 'r'))
        next(continentslatlon_csv)  #skip first row with headers
        for row in continentslatlon_csv:
            k, c, v1, v2,v3  = row
            CountlatlonDict[k] = c, float(v1),float(v2),str(v3)
        return CountlatlonDict
    except:
        pass

def load_laender():
    load_saved = 'laender.csv'
    landDict = dict()
    try:
        landDict_csv = csv.reader(open(load_saved, 'r'))
        next(landDict_csv)  #skip first row with headers
        for row in landDict_csv:
            k, lat, lon, name, pop  = row  # short,latitude,longitude,Name,Population
            landDict[k] = float(lat),float(lon),name, float(pop)
        return landDict
    except:
        pass

def load_countryshortlong():
    load_saved = 'countryshortlong.csv'
    CDict = dict()
    try:
        read_csv = csv.reader(open(load_saved, 'r'))
        next(read_csv)  #skip first row with headers
        for row in read_csv:
            k, c   = row
            CDict[k] = c
        return CDict
    except:
        pass

def load_countryhealth():
    load_saved = 'GlobalHealthExpenditures2016.csv'
    CDict = dict()
    try:
        read_csv = csv.reader(open(load_saved, 'r'))
        next(read_csv)  #skip first row with headers
        for row in read_csv:
            k, c   = row
            if c == '':
                c=0
            CDict[k] = float(c)

        return CDict
    except:
        pass

def move_stuff_to_google_sheets(df=None,spreadsheet_key='',wks_name='',text='',row_names=False):
    ## now to google
    import gspread
    from df2gspread import df2gspread as d2g
    from oauth2client.service_account import ServiceAccountCredentials

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('googlecoviddata.json', scope)
    gc = gspread.authorize(credentials)

    print("***LOADING TO GOOGLE ***")
    d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=row_names)
    print("***LOADING TO GOOGLE - DONE ***")
    print("*** DONE WITH", str(text), " ***")

def my_converter(val):
    try:
        return str(val)
    except ValueError:
        return val

def produce_covidfile():
    #read in the main datafile from github
    print("READING DATA...")
    df_comb = pandas.read_csv('https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv')
    print("READING DATA - DONE")

    # print(df_comb)

    dict_continents = load_continent_latlon()
    # print(dict_continents)

    dict_countries = load_countries_latlon()
    # print(dict_countries)

    #let's add the continent to the country, and the country geo info
    df_comb['continent'] = df_comb['Country/Region'].map(lambda x: dict_countries[x][0])
    df_comb['country-lat'] = df_comb['Country/Region'].map(lambda x: dict_countries[x][1])
    df_comb['country-lon'] = df_comb['Country/Region'].map(lambda x: dict_countries[x][2])
    df_comb['countryshort'] = df_comb['Country/Region'].map(lambda x: dict_countries[x][3])
    # now add the continent geo info as well
    df_comb['continent-lat'] = df_comb['continent'].map(lambda x: dict_continents[x][0])
    df_comb['continent-lon'] = df_comb['continent'].map(lambda x: dict_continents[x][1])

    #check the province, replace by country if necessary
    df_comb.loc[(pandas.isna(df_comb['Province/State'])), 'Province/State'] = "["+df_comb['Country/Region']+"]"
    #https://kanoki.org/2019/07/17/pandas-how-to-replace-values-based-on-conditions/

    #for Country/region Cruise Ship replace by Undefined
    df_comb.loc[(df_comb['Country/Region'] == 'Cruise Ship'), 'Province/State'] = "Undefined"
    df_comb.loc[(df_comb['Country/Region'] == 'Cruise Ship'), 'Country/Region'] = "Undefined"
    df_comb.loc[(df_comb['Country/Region'] == 'Diamond Princess'), 'Country/Region'] = "Undefined"
    df_comb.loc[(df_comb['Country/Region'] == 'MS Zaandam'), 'Country/Region'] = "Undefined"

    #change From Diamond Princess to Diamond Princess & country
    df_comb.loc[(df_comb['Province/State'] == 'From Diamond Princess'), 'Province/State'] = 'Diamond Princess-' + df_comb['Country/Region']
    df_comb.loc[(df_comb['Province/State'] == 'Grand Princess'), 'Province/State'] = 'Grand Princess-' + df_comb['Country/Region']
    df_comb.loc[(df_comb['Province/State'] == 'Diamond Princess'), 'Province/State'] = 'Diamond Princess-' + \
                                                                                            df_comb['Country/Region']

    #some renaming
    df_comb.rename(columns = {'Lat':'province-lat','Long':'province-lon' },inplace=True)

    #replacing nan by 0
    df_comb.loc[(pandas.isna(df_comb['Confirmed'])), 'Confirmed'] = 0
    df_comb.loc[(pandas.isna(df_comb['Recovered'])), 'Recovered'] = 0
    df_comb.loc[(pandas.isna(df_comb['Deaths'])), 'Deaths'] = 0
    #deal with Canada recovery data
    df_comb.loc[(df_comb['Province/State'] == 'Recovery aggregated') & (df_comb['Country/Region'] == 'Canada'), 'Province/State'] = "[Canada - Recovery only]"


    #add the active cases
    df_comb['Active']=df_comb['Confirmed']-df_comb['Recovered']-df_comb['Deaths']


    ##write the output to file
    df_comb.to_csv("COVID19output.csv",index=False)

    #what have we got?
    print(df_comb)

    ## now to google
    move_stuff_to_google_sheets(df=df_comb,spreadsheet_key='1105qUpQdFZfQ3L44WVI_kZO72KKfXqZhhusWLOk_qhA', wks_name='COVID19data', text='COVID19DATA',row_names=False)

def produce_countryfile():
    #population data into separate dataframe
    url = 'https://gist.githubusercontent.com/gwillem/6ca8a81048e6f3721c3bafc803d44a72/raw/4fb66d18178c1a0fdf101fb6b03c4d21929472da/iso2_population.json'
    req = urllib.request.Request(url)

    ##parsing response
    r = urllib.request.urlopen(req).read()
    population = json.loads(r.decode('utf-8'))

    #print(population['AD'])
    #df_comb['Population'] = df_comb['countryshort'].map(population)

    df_population = pandas.DataFrame.from_dict(population, orient='index')
    df_population.columns=['Population']
    df_population.index.names=['countryshort']

    dict_ctryshortlon=load_countryshortlong()
    dict_ctryhealth=load_countryhealth()
    #https://data.worldbank.org/indicator/SH.XPD.CHEX.GD.ZS


    df_population['countrylong']=df_population.index.map(dict_ctryshortlon)
    df_population['healthpercgdp']=df_population['countrylong'].map(dict_ctryhealth)

    print(df_population)
    df_population.to_csv("COVID19country.csv", index=True)

def produce_covidflights():
    from opensky_api import OpenSkyApi
    from datetime import datetime
    import time

    api = OpenSkyApi()
    # states = api.get_states()
    # print("all flights:", len(states.states))
    # df_flights = pandas.DataFrame(states.states)
    # flights_time = states.time
    # df_flights.to_csv("COVID19flights" + str(datetime.fromtimestamp(flights_time)).replace(':', '_') + ".csv",
    #                  index=True)
    # for s in states.states:
    #    print("(%r, %r, %r, %r)" % (s.longitude, s.latitude, s.baro_altitude, s.velocity))

    ## bbox = (min latitude, max latitude, min longitude, max longitude)
    print("*** READING NA flights")
    states_NA = api.get_states(bbox=(21.9,69,-160,-56))
    number_NA= len(states_NA.states)
    print("*** Done: number is:",number_NA )
    print("*** READING EU flights")
    time.sleep(15) # wait 15 secs for API to not block
    states_EU = api.get_states(bbox=(36, 69, -9, 37))
    number_EU= len(states_EU.states)
    print("*** Done: number is:", number_EU)

    flights_time_NA = datetime.fromtimestamp(states_NA.time)
    dfdata = {'North America flights':number_NA,'Europe flights':number_EU }
    dfindex = [flights_time_NA]
    df_flight_stats=pandas.DataFrame(dfdata, index=dfindex)
    df_flight_stats.index.names=['timestamp MST']
    df_flight_stats.to_csv("COVID19flightstats.csv", mode = 'a', index=True, header = False)

    df_flight_stats_all =pandas.read_csv("COVID19flightstats.csv")
    print(df_flight_stats_all)

    ## now to google
    move_stuff_to_google_sheets(df=df_flight_stats_all,spreadsheet_key='1Reb5-uZ77phwHMBUvICFyr6mLXnG_SbaHW41YLfrBYY', wks_name='COVID19flightstats',
                                text='COVID19DATA', row_names=False)

def produce_covid30daygtrends():
    from pytrends.request import TrendReq
    # see https: // pypi.org / project / pytrends /

    pytrend = TrendReq(hl='en-US', tz="420")  # offset -7h MST

    # %2Fm%2F09gtd,%2Fm%2F01jpn4,%2Fm%2F07l88z,%2Fm%2F034qg
    # ['/m/09gtd','/m/01jpn4','/m/07l88z','/m/034qg']
    # are the topics ['Toilet Paper', 'Grocery Store', 'Lockdown','Firearm']
    ## e.g. https://trends.google.com/trends/explore?date=today%203-m&geo=CA&q=%2Fm%2F09gtd,%2Fm%2F01jpn4,%2Fm%2F07l88z,%2Fm%2F034qg

    pytrend.build_payload(kw_list=['/m/09gtd', '/m/01jpn4', '/m/07l88z', '/m/034qg'], timeframe='today 3-m', geo='US')
    df_us = pytrend.interest_over_time()
    df_us.columns = ['Toilet Paper', 'Grocery Store', 'Lockdown', 'Firearm', 'checkpartial']
    df_us.index.names = ['date']
    df_us['Country'] = 'US'
    print(df_us.tail(10))
    pytrend.build_payload(kw_list=['/m/09gtd', '/m/01jpn4', '/m/07l88z', '/m/034qg'], timeframe='today 3-m', geo='CA')
    df_can = pytrend.interest_over_time()
    df_can.columns = ['Toilet Paper', 'Grocery Store', 'Lockdown', 'Firearm', 'checkpartial']
    df_can.index.names = ['date']
    df_can['Country'] = 'CA'
    print(df_can.tail(10))


    ## now to google
    move_stuff_to_google_sheets(df=df_us,spreadsheet_key='1w0Lj0ZcJ-4nHrnR6KtYLrNnddartYcN19OFg5UXZuqM', wks_name='COVID19ustrends',
                                text='COVID19trends US', row_names=True)
    move_stuff_to_google_sheets(df=df_can,spreadsheet_key='1w0Lj0ZcJ-4nHrnR6KtYLrNnddartYcN19OFg5UXZuqM', wks_name='COVID19cantrends',
                                text='COVID19trends CAN', row_names=True)

def produce_doubling_rate():

    # from datetime import datetime, timedelta
    from math import log
    df_comb_test = pandas.read_csv("COVID19output.csv")
    df_confirmed_only = df_comb_test[['countryshort', 'Date', 'Confirmed']].copy()

    df_confirmed_bycandd = df_confirmed_only.groupby(['countryshort', 'Date']).sum().reset_index()

    # pivot to have the countries on top
    df_pivot = pandas.pivot_table(df_confirmed_bycandd, values='Confirmed', index=['Date'], columns='countryshort')
    # print(df_pivot)
    wind = 7  # days window

    # lambda contains the doubling formula. note the -1 index to get the last=latest value in the window
    df_doubling = df_pivot.rolling(window=wind).apply(lambda x: 0 if x[0] == 0 or x[-1] == -1 else (
        0 if (((x[-1] - x[0]) / x[0]) <= 0) else (wind * log(2) / (log(1 + (x[-1] - x[0]) / x[0])))))
    df_doubling2 = df_doubling.reset_index()
    df_doubling2.index.names = ['Date']

    # print(df_doubling)

    df_unpivoted = df_doubling2.melt(id_vars=['Date'], var_name='countryshort', value_name='doubling_rate')
    print(df_unpivoted)

    # since we are at it, do the relative growth as well
    df_growth = df_pivot.rolling(window=wind).apply(lambda x: 0 if x[0] == 0 or x[-1] == -1 else (
        (x[-1] - x[0]) / x[0]))
    df_growth2 = df_growth.reset_index()
    df_growth2.index.names = ['Date']

    # print(df_growth)

    df_unpivotedg = df_growth2.melt(id_vars=['Date'], var_name='countryshort', value_name='growth_rate')
    print(df_unpivotedg)

    ##same again, but with the death numbers:
    df_deaths_only = df_comb_test[['countryshort', 'Date', 'Deaths']].copy()

    df_deaths_bycandd = df_deaths_only.groupby(['countryshort', 'Date']).sum().reset_index()

    # pivot to have the countries on top
    df_pivotd = pandas.pivot_table(df_deaths_bycandd, values='Deaths', index=['Date'], columns='countryshort')
    # print(df_pivotd)
    wind = 7  # days window

    # lambda contains the doubling formula. note the -1 index to get the last=latest value in the window
    df_doublingd = df_pivotd.rolling(window=wind).apply(lambda x: 0 if x[0] == 0 or x[-1] == -1 else (
        0 if (((x[-1] - x[0]) / x[0]) <= 0) else (wind * log(2) / (log(1 + (x[-1] - x[0]) / x[0])))))
    df_doubling2d = df_doublingd.reset_index()
    df_doubling2d.index.names = ['Date']

    # print(df_doublingd)

    df_unpivotedd = df_doubling2d.melt(id_vars=['Date'], var_name='countryshort', value_name='doubling_rate_deaths')
    print(df_unpivotedd)

    # since we are at it, do the relative growth as well
    df_growthd = df_pivotd.rolling(window=wind).apply(lambda x: 0 if x[0] == 0 or x[-1] == -1 else (
        (x[-1] - x[0]) / x[0]))
    df_growth2d = df_growthd.reset_index()
    df_growth2d.index.names = ['Date']

    # print(df_growthd)

    df_unpivotedgd = df_growth2d.melt(id_vars=['Date'], var_name='countryshort', value_name='growth_rate_deaths')
    print(df_unpivotedgd)

    ## now to google
    move_stuff_to_google_sheets(df=df_unpivoted, spreadsheet_key='1mlBxqhBC9fCAsBuJ8Dtn1mJS-B89X2ulX5Wrgpyya10', wks_name='COVID19doubling',
                                text='COVID19doubling', row_names=False)
    move_stuff_to_google_sheets(df=df_unpivotedg,spreadsheet_key='1mlBxqhBC9fCAsBuJ8Dtn1mJS-B89X2ulX5Wrgpyya10', wks_name='COVID19growth',
                                text='COVID19growth', row_names=False)

    move_stuff_to_google_sheets(df=df_unpivotedd, spreadsheet_key='1mlBxqhBC9fCAsBuJ8Dtn1mJS-B89X2ulX5Wrgpyya10', wks_name='COVID19doubling_deaths',
                                text='COVID19doubling-Deaths', row_names=False)
    move_stuff_to_google_sheets(df=df_unpivotedgd,spreadsheet_key='1mlBxqhBC9fCAsBuJ8Dtn1mJS-B89X2ulX5Wrgpyya10', wks_name='COVID19growth_deaths',
                                text='COVID19growth-Deaths', row_names=False)

def produce_us_counties():
    url='https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    # from nytimes ,https://www.nytimes.com/interactive/2020/us/coronavirus-us-cases.html#g-us-map
    print("READING DATA...")
    df_usc = pandas.read_csv(url,converters={'fips':my_converter}) # previously, fips were output as decimal
    print("READING DATA - DONE")
    print(df_usc)
    ## now to google

    move_stuff_to_google_sheets(df=df_usc,spreadsheet_key='1Egz51XT2P7tclI00CQmGDsBwT_OC8-Ljf7ggwReDA_k', wks_name='COVID19us_counties',
                                text='COVID19us_counties', row_names=False)

def produce_de_laender():
    import datetime as dt
    url='https://raw.githubusercontent.com/jgehrcke/covid-19-germany-gae/master/data.csv'
    # see https://github.com/jgehrcke/covid-19-germany-gae
    # df_del = pandas.read_csv(url, parse_dates=["time_iso8601"])
    df_del = pandas.read_csv(url)
    df_del['Date']=df_del['time_iso8601'].str[0:10]
    df_del.reset_index()
    # df_del.set_index(['Date'],inplace=True)
    df_del.drop(columns=['time_iso8601','source','sum_cases','sum_deaths'],inplace=True)
    # now unpivot (melt) and start to create rows per state per day
    df_del2= pandas.melt(df_del,var_name='laender_data',value_name='number',id_vars=['Date'],value_vars=['DE-BW_cases','DE-BW_deaths','DE-BY_cases','DE-BY_deaths','DE-BE_cases','DE-BE_deaths','DE-BB_cases','DE-BB_deaths','DE-HB_cases','DE-HB_deaths','DE-HH_cases','DE-HH_deaths','DE-HE_cases','DE-HE_deaths','DE-MV_cases','DE-MV_deaths','DE-NI_cases','DE-NI_deaths','DE-NW_cases','DE-NW_deaths','DE-RP_cases','DE-RP_deaths','DE-SL_cases','DE-SL_deaths','DE-SN_cases','DE-SN_deaths','DE-SH_cases','DE-SH_deaths','DE-ST_cases','DE-ST_deaths','DE-TH_cases','DE-TH_deaths'])
    mask = df_del2['laender_data'].str.contains(r'_cases', na=True)
    df_del2.loc[mask, 'Type'] = 'Confirmed'
    mask = df_del2['laender_data'].str.contains(r'_deaths', na=True)
    df_del2.loc[mask, 'Type'] = 'Deaths'
    df_del2['Land']=df_del2['laender_data'].str[3:5]
    df_del2.drop(columns=['laender_data'], inplace=True)
    # now partially pivot back into a form that we prefer for the sheet layout
    df_del3=pandas.pivot_table(df_del2,index=['Date','Land'],values=['number'],columns=['Type'])
    df_del3.reset_index(inplace=True)
    df_del3.columns.to_flat_index()
    df_del3.columns = ['_'.join(tuple(map(str, t))) for t in df_del3.columns.values]
    df_del3.rename(columns={'Date_': 'Date', 'Land_': 'Land','number_Confirmed': 'Confirmed', 'number_Deaths': 'Deaths'}, inplace=True)
    # now map some laender master data and geo data
    dict_laender = load_laender()
    df_del3['Name'] = df_del3['Land'].map(lambda x: dict_laender[x][2])
    df_del3['Lat'] = df_del3['Land'].map(lambda x: dict_laender[x][0])
    df_del3['Lon'] = df_del3['Land'].map(lambda x: dict_laender[x][1])
    df_del3['Population'] = df_del3['Land'].map(lambda x: dict_laender[x][3])

    print(df_del3)
    # now to google

    move_stuff_to_google_sheets(df=df_del3,spreadsheet_key='1ddfX66l3VjuRK1iRp1fr5kd4b7L9RqIW0f7xUtKTvkE', wks_name='COVID19de_laender',
                                text='COVID19de_laaender', row_names=False)

def produce_day_of_100_compare():

    # from datetime import datetime, timedelta
    from math import log
    df_comb_test = pandas.read_csv("COVID19output.csv")
    df_confirmed_only = df_comb_test[['countryshort', 'Date', 'Confirmed']].copy()
    # print(df_confirmed_only)
    ##df_confirmed_only['date'] = pandas.to_datetime(df_confirmed_only['Date'])
    ##df_confirmed_only.drop(['Date'], axis=1, inplace=True)

    df_confirmed_bycandd = df_confirmed_only.groupby(['countryshort', 'Date']).sum().reset_index()
    # last_date=df_confirmed_bycandd['Date'].max()  # is a str
    # last_date_dt = datetime.strptime(last_date, '%Y-%m-%d').date()
    # first_date = last_date_dt-timedelta(window)

    # pivot to have the countries on top
    df_pivot = pandas.pivot_table(df_confirmed_bycandd, values='Confirmed', index=['Date'], columns='countryshort')
    df_pivot.reset_index(inplace=True)
    df_pivot.drop('Date',inplace=True,axis=1)
    df_shortened = pandas.DataFrame(df_pivot.index.tolist())
    for column in df_pivot.columns:
        df_help=df_pivot[column].reindex()
        df_help2=df_help[df_help>=100].reset_index()
        # df_shortened[column] = df_help2[column].map(lambda x : log(x,10))
        df_shortened[column] = df_help2[column]  # we don't want the log
    df_shortened.drop([0],inplace=True,axis=1)
    df_shortened.index.name='Days_from_100_cases'
    df_shortened.reset_index(inplace=True)
    # print(df_shortened)
    df_unpivoted = df_shortened.melt(id_vars=['Days_from_100_cases'],var_name='countryshort', value_name='cases')
    df_unpivoted.dropna(inplace=True)
    print(df_unpivoted)


    ## same again for the number of days from 10th death
    df_deaths_only = df_comb_test[['countryshort', 'Date', 'Deaths']].copy()

    df_deaths_bycandd = df_deaths_only.groupby(['countryshort', 'Date']).sum().reset_index()

    # pivot to have the countries on top
    df_pivotd = pandas.pivot_table(df_deaths_bycandd, values='Deaths', index=['Date'], columns='countryshort')
    df_pivotd.reset_index(inplace=True)
    df_pivotd.drop('Date', inplace=True, axis=1)
    df_shortenedd = pandas.DataFrame(df_pivotd.index.tolist())
    for column in df_pivotd.columns:
        df_helpd = df_pivotd[column].reindex()
        df_help2d = df_helpd[df_helpd >= 10].reset_index()
        # df_shortenedd[column] = df_help2d[column].map(lambda x: log(x, 10))
        df_shortenedd[column] = df_help2d[column] # we don't want the log
    df_shortenedd.drop([0], inplace=True, axis=1)
    df_shortenedd.index.name = 'Days_from_10_deaths'
    df_shortenedd.reset_index(inplace=True)
    # print(df_shortenedd)
    df_unpivotedd = df_shortenedd.melt(id_vars=['Days_from_10_deaths'], var_name='countryshort', value_name='deaths')
    df_unpivotedd.dropna(inplace=True)
    print(df_unpivotedd)

    # now to google
    move_stuff_to_google_sheets(df=df_unpivoted,spreadsheet_key='15Hum7bNcHnw-hUA3B5hLKXMDYRIwkQSYWIcZ49yrDAk', wks_name='COVID19comparelog',
                                text='COVID19comparelog Confirmed', row_names=False)
    move_stuff_to_google_sheets(df=df_unpivotedd,spreadsheet_key='15Hum7bNcHnw-hUA3B5hLKXMDYRIwkQSYWIcZ49yrDAk', wks_name='COVID19comparelogdeaths',
                                text='COVID19comparelog Deaths', row_names=False)


    #
    # same again for the number of days from 1000th death, just for plotting
    #
    df_deaths_only = df_comb_test[['countryshort', 'Date', 'Deaths']].copy()

    df_deaths_bycandd = df_deaths_only.groupby(['countryshort', 'Date']).sum().reset_index()

    # pivot to have the countries on top
    df_pivotd = pandas.pivot_table(df_deaths_bycandd, values='Deaths', index=['Date'], columns='countryshort')
    df_pivotd.reset_index(inplace=True)
    df_pivotd.drop('Date', inplace=True, axis=1)
    df_shortenedd = pandas.DataFrame(df_pivotd.index.tolist())
    for column in df_pivotd.columns:
        df_helpd = df_pivotd[column].reindex()
        df_help2d = df_helpd[df_helpd >= 1000].reset_index()
        df_shortenedd[column] = df_help2d[column] #.map(lambda x: log(x, 10))
    df_shortenedd.drop([0], inplace=True, axis=1)
    df_shortenedd.index.name = 'Days_from_1000_deaths'
    df_shortenedd.reset_index(inplace=True)
    # print(df_shortenedd)
    df_unpivotedd = df_shortenedd.melt(id_vars=['Days_from_1000_deaths'], var_name='countryshort', value_name='deaths')
    df_unpivotedd.dropna(inplace=True)
    print(df_unpivotedd)

    #a sample plot, python does the log scaling internally...
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    for label, grp in df_unpivotedd.groupby('countryshort'):
        grp.plot(x='Days_from_1000_deaths', y='deaths', ax=ax, label=label, logy=True,figsize=(10,8),legend=True).set_ylabel("Number of Deaths (log scale)")
    plt.show()

if __name__ == '__main__':
    now = datetime.datetime.now()
    print("******     START            ***** ",now.strftime("%Y-%m-%d %H:%M:%S"))
    ## produce_countryfile()  ## only needed one time
    ## core data
    produce_covidfile()
    produce_doubling_rate()    # needs covidfile output csv first
    produce_day_of_100_compare()
    more details for some countries
    produce_us_counties()
    produce_de_laender()
    ## optional extras
    produce_covidflights()
    produce_covid30daygtrends()


    now = datetime.datetime.now()
    print("******     THE     END      ***** ",now.strftime("%Y-%m-%d %H:%M:%S"))

















