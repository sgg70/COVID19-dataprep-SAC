# https://medium.com/analytics-vidhya/mapping-the-spread-of-coronavirus-covid-19-d7830c4282e

# Starting the required libraries
import numpy as np
import geopandas as gpd
import pandas as pd
from functools import reduce
import plotly_express as px

#### Part 1 : Preparing the data
# 1.1 Downloading csv into dataframe
df_confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
df_deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
df_recovered = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')

df_confirmed.head(5)

# 1.2 Tidying the data
# Using melt() command in pandas (similar to gather() in R's tidyr)
id_list = df_confirmed.columns.to_list()[:4]
vars_list = df_confirmed.columns.to_list()[4:]
confirmed_tidy = pd.melt(df_confirmed, id_vars=id_list,\
     value_vars=vars_list, var_name='Date', value_name='Confirmed')
deaths_tidy = pd.melt(df_deaths, id_vars=id_list,\
     value_vars=vars_list, var_name='Date', value_name='Deaths')
recovered_tidy = pd.melt(df_recovered, id_vars=id_list,\
     value_vars=vars_list, var_name='Date', value_name='recovered')
# 1.3 Merging the three dataframes into one
data_frames = [confirmed_tidy, deaths_tidy, recovered_tidy]
df_corona = reduce(lambda left, right: pd.merge(left, right, on =\
               id_list+['Date'], how='outer'), data_frames)
# 1.4 Each row should only represent one observation
id_vars = df_corona.columns[:5]
data_type = ['Confirmed', 'Deaths', 'recovered']
df_corona = pd.melt(df_corona, id_vars=id_vars,\
          value_vars=data_type, var_name='type', value_name='Count')
df_corona['Date'] = pd.to_datetime(df_corona['Date'],\
            format='%m/%d/%y', errors='raise')



#sums
corona_sums = df_corona.groupby(['type', 'Date'],\
                     as_index=False).agg({'Count':'sum'})
print(corona_sums)

def plot_timeseries(df):
    fig = px.line(df, x='Date', y='Count', color='type',\
             template='plotly_dark')

    fig.update_layout(legend_orientation="h")
    return(fig)

# fig = plot_timeseries(corona_sums)
# fig.show()


# Selecting only the Confirmed cases
tsmap_corona = df_corona[df_corona['type']=='Confirmed']
tsmap_corona['Date'] = tsmap_corona['Date'].astype(str)

# Classifying data for visulalization
to_Category = pd.cut(tsmap_corona['Count'], [-1,0,105, 361, 760,\
            1350, 6280, 200000], labels=[0, 1, 8, 25, 40, 60, 100])
tsmap_corona = tsmap_corona.assign(size=to_Category)

fig_time= px.scatter_mapbox(data_frame=tsmap_corona,lat='Lat',lon='Long',\
        hover_name= 'Country/Region', hover_data=['Province/State',\
        'Count'], size='size', animation_frame='Date',\
       mapbox_style='stamen-toner', template='plotly_dark', zoom=1,\
       size_max=70)
fig_time.show()

