import geopandas as gpd
import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table
from geoalchemy2 import Geometry
from geopy.geocoders import Nominatim
from time import sleep
import pandas as pd
import requests
import pyproj

# Set the working directory to ensure relative paths work correctly
os.chdir('e:\\SE4GEO\\hoda\\hoda')
# read the csv to the dataframe
target_cities = pd.read_csv('data/target_cities.csv')

# Download data for each uid
def get_data_by_uid(uid):
    try:
        # Construct the API request URL
        api_url = f'https://test.idrogeo.isprambiente.it/api/pir/comuni/{uid}'

        # Fetch data from the API
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Convert the response JSON data into a Python dictionary
            data = response.json()

            # Convert the dictionary into a Pandas DataFrame
            df = pd.json_normalize(data)

            # Return the DataFrame
            return df
        else:
            # Print an error message and return None
            print("Failed to fetch data from the API. Status code:", response.status_code)
            return None
    except requests.RequestException as e:
        # Handle request exceptions
        print(f"An error occurred while making the API request: {e}")
        return None
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred while processing the data: {e}")
        return None

def merge_dataframes(dataframes):
    try:
        # Check if the list of DataFrames is not empty
        if dataframes:
            # Merge DataFrames using pandas concat function
            merged_df = pd.concat(dataframes, ignore_index=True)
            return merged_df
        else:
            # If the list is empty, return an empty DataFrame
            return pd.DataFrame()
    except Exception as e:
        # Handle exceptions during merging
        print(f"An error occurred while merging dataframes: {e}")
        return pd.DataFrame()

def fetch_api_data(rip, reg, prov):
    try:
        # Construct the API request URL
        api_url = f'https://test.idrogeo.isprambiente.it/api/pir/comuni?cod_rip={rip}&cod_reg={reg}&cod_prov={prov}'

        # Fetch data from the API
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Convert the response JSON data into a Python dictionary
            data = response.json()

            # Convert the dictionary into a Pandas DataFrame
            df = pd.json_normalize(data)

            # Return the DataFrame
            return df
        else:
            # Print an error message and return None
            print("Failed to fetch data from the API. Status code:", response.status_code)
            return None
    except requests.RequestException as e:
        # Handle request exceptions
        print(f"An error occurred while making the API request: {e}")
        return None
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred while processing the data: {e}")
        return None

def extract_uids(df):
    try:
        # Extract 'uid' column from DataFrame and convert to list
        uids_list = df['uid'].tolist()
        return uids_list
    except KeyError as e:
        # Handle key error if 'uid' column is missing
        print(f"The DataFrame does not contain the 'uid' column: {e}")
        return []
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred while extracting UIDs: {e}")
        return []

def download_data_as_dataframe(endpoint_url, timeout=10):
    """
    Download data from an API endpoint and convert it to a pandas DataFrame.

    Args:
        endpoint_url (str): The URL of the API endpoint.
        timeout (int, optional): Timeout for the HTTP request in seconds (default: 10).

    Returns:
        pandas.DataFrame: DataFrame containing the downloaded data, or None if an error occurs.
    """
    try:
        # Send a GET request to the endpoint with a timeout
        response = requests.get(endpoint_url, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Convert JSON response to DataFrame
        data = response.json()
        df = pd.DataFrame(data)

        return df

    except requests.exceptions.RequestException as e:
        # Log the error
        print(f"Error downloading data from {endpoint_url}: {e}")
        return None

def utm_to_latlon(easting, northing, zone_number=32, zone_letter='N'):
    proj = pyproj.Proj(proj='utm', zone=zone_number, ellps='WGS84', datum='WGS84')
    lon, lat = proj(easting, northing, inverse=True)
    return lat, lon


# Initialize geolocator
geolocator = Nominatim(user_agent="city_geocoder")

# List to store city coordinates
city_coordinates = []

# Loop through each city and get its coordinates
for city in target_cities['name']:
    location = geolocator.geocode(city)
    if location:
        city_coordinates.append([city, location.latitude, location.longitude])
    else:
        city_coordinates.append([city, None, None])
    sleep(1)

# Create a DataFrame
cities_latlon_df = pd.DataFrame(city_coordinates, columns=['name', 'lat', 'lon'])

# merge the target_cities with the cities_latlon_df
target_cities = target_cities.merge(cities_latlon_df, left_on='name', right_on='name')

# Specify primary data columns
city_dataframes = []

for uid in target_cities['uid']:
    # Fetch additional data for each uid
    data_by_uid = get_data_by_uid(uid)
    city_dataframes.append(data_by_uid)

# Merge all DataFrames into a single DataFrame
final_city_data = merge_dataframes(city_dataframes)
# Drop unnecessary columns
final_city_data.drop(columns=['nome','osmid', 'breadcrumb', 'extent', 'cod_rip', 'cod_reg', 'cod_prov', 'pro_com'], inplace=True)
# merge the final_city_data with the target_cities
final_city_data = target_cities.merge(final_city_data, left_on='uid', right_on='uid')

# Explicitly set dtype for each column
column_dtype_mapping = {'name': str,'uid': int}

# Set other columns to float
for col in final_city_data.columns:
    if col not in column_dtype_mapping:
        column_dtype_mapping[col] = float

final_city_data = final_city_data.astype(column_dtype_mapping)

# Convert to GeoDataFrame
final_city_gdf = gpd.GeoDataFrame(final_city_data, geometry=gpd.points_from_xy(final_city_data.lon, final_city_data.lat))
#define Crs
final_city_gdf.crs = 'EPSG:4326'
print("Data processing and integration completed successfully.")

# Database connection
# Establish a connection to the PostgreSQL database using the provided URL
db_url = 'postgresql://postgres:admin@localhost:5432/hoda'
engine = create_engine(db_url)
con = engine.connect()

# Removing existing tables
# Reflect all tables existing in the database
metadata = MetaData()
metadata.reflect(bind=engine)
# Define a list of tables related to PostGIS that should not be dropped
postgis_tables = ['spatial_ref_sys', 'geometry_columns']  # Add more if necessary
# Drop each table except those related to PostGIS
for table_name in metadata.tables.keys():
    if table_name not in postgis_tables:
        table = Table(table_name, metadata, autoload=True, autoload_with=engine)
        table.drop(engine)
# Notify when all non-PostGIS tables are successfully dropped
print("All non-PostGIS tables dropped from the database.")
# Exit the program or handle the error appropriately
# Insert processed city data into the database
# Insert the processed city geographical data into the 'cities' table, replacing existing data if any
final_city_gdf.to_postgis('CITY', engine, if_exists='replace', index=False, dtype={'geometry': Geometry('POINT', srid=4326)})
print("Final City Data inserted into the database successfully.")

con.close()
# Notify when the database connection is closed
print("Database connection closed.")