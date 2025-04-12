import requests
import pandas as pd
import numpy as np
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

# Configure the logging module
logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    filename="app2.log"
)

def extract_json_from_url(url:str):
    try:
        return requests.get(url).json()
    except Exception as e:
        raise ConnectionError(f"Can't extract data : {e}")
    
def covid_data():
    covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
    covid_json = extract_json_from_url(covid_url)
    all_countries_lst = []
    for country_short_code in covid_json.keys():
        single_country_df = pd.json_normalize(covid_json[country_short_code])
        single_country_df['iso_country_code'] = country_short_code
        # all_countries_df = pd.concat([all_countries_df, single_country_df], ignore_index=True)
        all_countries_lst.append(single_country_df)

    all_countries_df = pd.concat(all_countries_lst, ignore_index=True)    
    # drop all blank columns
    all_countries_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    all_countries_df.dropna(how='all', axis=1, inplace=True)
    ## Select only required columns (first 10)
    selected_cols = ["iso_country_code", "continent", "location", "last_updated_date", "total_cases", "new_cases", "total_deaths", "new_deaths", "total_cases_per_million", "total_deaths_per_million", "hosp_patients"]
    selected_df = all_countries_df[selected_cols]
    
    return selected_df


#['id', 'name', 'state_id', 'state_code', 'state_name', 'country_id',
       #'country_code', 'country_name', 'latitude', 'longitude', 'wikiDataId'
def cities_data():
    cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bstates%2Bcities.json"
    cities_json = extract_json_from_url(cities_url)
    cities_lst = []
    for country in cities_json:
        data_dict ={
            'country_id' : country.get('id', np.nan),
            'country_name' : country.get('name', np.nan),
            'country_iso3' : country.get('iso3', np.nan),
            'country_capital': country.get('capital', np.nan),
            'country_subregion': country.get('subregion', np.nan),
            'country_region': country.get('region', np.nan),
        }
        cities_lst.append(data_dict)
    citise_df = pd.DataFrame(cities_lst)
    return citise_df

#https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
def single_city_weather(city_name:str):
    api_key = os.getenv("WEATHER_KEY")
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    try:
       weather_json = extract_json_from_url(weather_url)
       #logging.debug(f"Weather API Response: {weather_json}")
       if not weather_json or "weather" not in weather_json or "main" not in weather_json:
        logging.warning(f"Invalid response for {city_name}: {weather_json}")
        return None 
       weather_data_dict = {
        "condition" : weather_json['weather'][0].get('description', "Unknown weather"),
        "temperature_min" : weather_json['main'].get('temp_min' , np.nan),
        "temperature_max" : weather_json['main'].get('temp_max' , np.nan)
       }
       weather_data_dict['city'] = city_name

       return weather_data_dict
    except ConnectionError:
        logging.debug(f"Can't get data from this city : {city_name}")
        return None
    except Exception as e:
        logging.info(f"Other error - {e}")
        return None

def all_cities_weather(cities_lst:list):
    all_cities =[]
    for city_name in cities_lst:
        if city_dict := single_city_weather(city_name):
            all_cities.append(city_dict)
        else:
            logging.info(f"This city {city_name} is being none!")
    all_city_df = pd.DataFrame(all_cities)
    return all_city_df


   
def transform_data():
    cities_df = cities_data()
    cities_name = cities_df['country_capital'].to_list()
    city_weather_df = all_cities_weather(cities_name)
    
     ## Join city_df with weather
    city_weather_df = cities_df.merge(city_weather_df, how="inner", left_on='country_capital', right_on='city')
   # print(f"Joined result - {city_weather_df.shape}")
    
    ## Join covid_df with joined results
    covid_df = covid_data()
    covid_city_weather_df = covid_df.merge(city_weather_df, how="left", left_on="iso_country_code", right_on="country_iso3")
    return covid_city_weather_df

def load_data(df:pd.DataFrame):
    engine = create_engine('sqlite:///w9_demo.db')
    df.to_sql("covid_city_demo", engine, if_exists="replace")
    print("Successfully loaded into sqlite db!")

if __name__ == "__main__":
    df = transform_data()
    print(df.shape)
    print(df.head())
   
