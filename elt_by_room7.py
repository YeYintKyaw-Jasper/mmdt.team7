import requests
import pandas as pd
import numpy as np
import os
import logging
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
    all_countries_df = pd.DataFrame()
    for country_short_code in covid_json.keys():
        single_country_df = pd.json_normalize(covid_json[country_short_code])
        single_country_df['iso_country_code'] = country_short_code
        all_countries_df = pd.concat([all_countries_df, single_country_df], ignore_index=True)
        
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
    cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"
    cities_json = extract_json_from_url(cities_url)
    cities_lst = []
    for city in cities_json:
        data_dict ={
            'city_id' : city.get('id', np.nan),
            'city_name' : city.get('name', np.nan),
            'city_iso' : city.get('state_code', np.nan),
            'country_name' : city.get('country_name', np.nan),
            'city_latitude' : city.get('latitude', np.nan),
            'city_longitude' : city.get('longitude', np.nan)
        }
        cities_lst.append(data_dict)
    citise_df = pd.DataFrame(cities_lst)
    return citise_df

#https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
def single_city_weather(city_name:str):
    api_key = os.getenv("WEATHER_KEY")
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    weather_json = extract_json_from_url(weather_url)
    logging.debug(f"Weather API Response: {weather_json}")
    weather_data_dict = {
        "condition" : weather_json["weather"][0].get('description', "Unknown weather"),
        "temperature_min" : weather_json['main'].get('temp_min' , np.nan),
        "temperature_max" : weather_json['main'].get('temp_max' , np.nan)
    }
    weather_data_dict['city'] = city_name

    return weather_data_dict

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
    cities_name = cities_df['city_name'].to_list()
    city_weather_df = all_cities_weather(cities_name)
    return city_weather_df


if __name__ == "__main__":
    df = transform_data()
    print(df.shape)
    print(df.head)
   

