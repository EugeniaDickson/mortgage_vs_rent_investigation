
import pandas as pd
import numpy as np

# SF Metro
sf_counties = ['Alameda County', 'Contra Costa County', 'Marin County', 'Napa County', 'San Mateo County', 
               'Santa Clara County', 'Solano County', 'Sonoma County', 'San Francisco County']
# NY Metro:
ny_counties = ['New York County', 'Bronx County', 'Queens County', 'Kings County', 'Richmond County']
# Greater Austin Metro:
tx_counties = ['Travis County']
# Miami Metro:
mia_counties = ['Miami-Dade County', 'Broward County', 'Palm Beach County']

counties_dict = {'CA':sf_counties,'NY':ny_counties,'TX':tx_counties,'FL':mia_counties}

all_counties = []
for state,counties in counties_dict.items():
    for county in counties:
        all_counties.append('%s-%s' % (state,county))


def transform_zori(path):
    '''
    Transforms the Zillow ZRI data file:
        - imputes missing rents by interpolation and backfilling
        - transforms date/rent columns into rows

    Args:
    path: path to the data file, str

    Merge By: 
        time: Date (01 of every month), Year
        location: State, City, Metro, County, Zipcode

    '''
    dataframe = pd.read_csv(path, dtype = {'RegionName':str})
    dataframe.rename(columns = {'RegionName':'Zipcode'}, inplace = True)
    dataframe['Zipcode'] = dataframe['Zipcode'].str.zfill(5)

    dataframe = pd.melt(dataframe, id_vars =dataframe.columns[:4],
                         value_vars = dataframe.columns[4:],
                         var_name = 'Date', 
                         value_name = 'Rent')
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])

    fill_rents = dataframe[['Zipcode', 'Rent', 'Date']].copy()

    # imputing missing Rent values
    fill_rents = fill_rents.reset_index().pivot(index = 'Date',columns = 'Zipcode')['Rent'].reset_index()
    fill_rents.fillna(method = 'ffill',inplace = True)
    fill_rents = pd.melt(fill_rents, id_vars='Date', 
                                      value_vars = fill_rents.columns[1:],
                                      var_name='Zipcode',value_name = 'Rent')
    dataframe.drop(['Rent', 'RegionID'], axis = 1, inplace = True)
    dataframe = pd.merge(dataframe, fill_rents, on = ['Date','Zipcode'])

    #parsing year separately for merging with annual features
    dataframe['Year'] = dataframe['Date'].dt.year
    
    date_cut = '2015-01-01'
    dataframe = dataframe[dataframe['Date']>=date_cut]

    nulls_df = dataframe[dataframe['Rent'].isnull()]
    bad_zips = nulls_df['Zipcode'].unique().tolist()
    dataframe = dataframe[dataframe['Zipcode'].isin(bad_zips)==False]

    return(dataframe)

def transform_zillow(path):
    '''
    Transforms the Zillow ZRI data file:
        - imputes missing rents by interpolation and backfilling
        - transforms date/rent columns into rows

    Args:
    path: path to the data file, str

    Merge By: 
        time: Date (01 of every month), Year
        location: State, City, Metro, County, Zipcode

    '''
    dataframe = pd.read_csv(path, dtype = {'RegionName':str})
    dataframe.rename(columns = {'RegionName':'Zipcode',
                                'CountyName': 'County'}, inplace = True)
    dataframe['Zipcode'] = dataframe['Zipcode'].str.zfill(5)

    dataframe = pd.melt(dataframe, id_vars =dataframe.columns[:7],
                         value_vars = dataframe.columns[7:],
                         var_name = 'Date', 
                         value_name = 'Rent')
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])

    fill_rents = dataframe[['Zipcode', 'Rent', 'Date']].copy()

    # imputing missing Rent values
    fill_rents = fill_rents.reset_index().pivot(index = 'Date',columns = 'Zipcode')['Rent'].reset_index()
    fill_rents.fillna(method = 'ffill',inplace = True)
    fill_rents = pd.melt(fill_rents, id_vars='Date', 
                                      value_vars = fill_rents.columns[1:],
                                      var_name='Zipcode',value_name = 'Rent')
    dataframe.drop(['Rent', 'RegionID'], axis = 1, inplace = True)
    dataframe = pd.merge(dataframe, fill_rents, on = ['Date','Zipcode'])

    #parsing year separately for merging with annual features
    dataframe['Year'] = dataframe['Date'].dt.year
    
    date_cut = '2015-01-01'
    dataframe = dataframe[dataframe['Date']>=date_cut]

    nulls_df = dataframe[dataframe['Rent'].isnull()]
    bad_zips = nulls_df['Zipcode'].unique().tolist()
    dataframe = dataframe[dataframe['Zipcode'].isin(bad_zips)==False]
    
    dataframe['State-County'] = dataframe['State'] + '-' + dataframe['County']
    dataframe = dataframe[dataframe['State-County'].isin(all_counties)]
    return(dataframe)