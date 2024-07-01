from dash import Dash, html, dcc, Output, Input
import geopandas as gpd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import Fullscreen
import pandas as pd
import requests
import pyproj

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

def utm_to_latlon(easting, northing, zone_number=32):
    proj = pyproj.Proj(proj='utm', zone=zone_number, ellps='WGS84', datum='WGS84')
    lon, lat = proj(easting, northing, inverse=True)
    return lat, lon

# Download cities data from the API endpoints
cities = download_data_as_dataframe("http://localhost:5005/api/comune")
#making geodataframe from lat lon columns
cities = gpd.GeoDataFrame(cities, geometry=gpd.points_from_xy(cities.lon, cities.lat))

map_path = 'e:\\SE4GEO\\hoda\\hoda\\map.html'

# Options for dropdowns
city_options = [{'label': city, 'value': city} for city in cities['name']]
plot_type_options = [{'label': plot_type[0], 'value': plot_type[1]} for plot_type in [('Line Plot', 'line'), ('Bar Plot', 'bar'), ('Scatter Plot', 'scatter')]]
#define a dictionary for dividing different colummns
groups={
    'population':['pop_idr_p1','pop_idr_p2','pop_idr_p3'],
    'families':['fam_idr_p1','fam_idr_p2','fam_idr_p3'],
    'building':['ed_idr_p1','ed_idr_p2','ed_idr_p3'],
    'surface_area':['ar_id_p1','ar_id_p2','ar_id_p3'],
}

# Initialize the Dash app
app = Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    # Title
    html.H1("City Data"),
    # city dropdown
    html.Label("Select a city:"),
    dcc.Dropdown(
        id='city-dropdown',
        options=city_options,
        placeholder="Select a city",
        value='Milano'
    ),
    # plot type dropdown
    html.Label("Select a plot type:"),
    dcc.Dropdown(
        id='plot-type-dropdown',
        options=plot_type_options,
        placeholder="Select a plot type",
        value='bar'
    ),
    # container for map and plot sections
    html.Div([
        html.Div([
            # map section on the left
            html.Div([
                html.Iframe(id='folium-map', srcDoc=open(map_path, 'r').read(), width='100%', height='800')
            ], style={'flex': '1', 'padding': '10px'}),
            # plot section on the right
            html.Div(id='plot-section', style={'flex': '1', 'padding': '10px'})
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'width': '100%'})
    ], style={'display': 'flex', 'justifyContent': 'center'})
])

@app.callback(
    Output('plot-section', 'children'),
    [Input('city-dropdown', 'value'),
     Input('plot-type-dropdown', 'value')]
)
def update_plots(selected_city, selected_plot_type):
    if selected_city is None or selected_plot_type is None:
        return html.Div("Please select both a city and a plot type.")
    
    city_data = cities[cities['name'] == selected_city]
    if city_data.empty:
        return html.Div(f"No data available for {selected_city}.")

    fig = make_subplots(rows=2, cols=2, subplot_titles=('Population', 'Families', 'Building', 'Surface Area'))

    for i, (group_name, columns) in enumerate(groups.items(), start=1):
        data = city_data[columns].iloc[0]
        if selected_plot_type == 'line':
            fig.add_trace(go.Scatter(x=columns, y=data, mode='lines+markers', name=group_name), row=(i-1)//2 + 1, col=(i-1)%2 + 1)
        elif selected_plot_type == 'bar':
            fig.add_trace(go.Bar(x=columns, y=data, name=group_name), row=(i-1)//2 + 1, col=(i-1)%2 + 1)
        elif selected_plot_type == 'scatter':
            fig.add_trace(go.Scatter(x=columns, y=data, mode='markers', name=group_name), row=(i-1)//2 + 1, col=(i-1)%2 + 1)

        fig.update_xaxes(title_text='Categories', row=(i-1)//2 + 1, col=(i-1)%2 + 1)
        fig.update_yaxes(title_text='Values', row=(i-1)//2 + 1, col=(i-1)%2 + 1)
        fig.update_layout(height=800, showlegend=True)
    
    fig.update_layout(
        title_text="City Data Overview",
        height=800,
        width=800,
        legend_title_text='Groups',
        title_x=0.5
    )

    return dcc.Graph(figure=fig)

# callback for updating the map
@app.callback(
    Output('folium-map', 'srcDoc'),
    [Input('city-dropdown', 'value')]
)
def update_map(selected_city):
    if selected_city is None:
        return None
    
    city_data = cities[cities['name'] == selected_city]
    if city_data.empty:
        return open(map_path, 'r').read()

    city = city_data.iloc[0]
    m = folium.Map(location=[city['lat'], city['lon']], zoom_start=12)
    folium.Marker([city['lat'], city['lon']], popup=f"<b>{city['name']}</b></br>population:{int(city['pop_idr_p1']+city['pop_idr_p2']+city['pop_idr_p3'])}</br>surface area:{int(city['ar_id_p1']+city['ar_id_p2']+city['ar_id_p3'])} Km2</br>building:{int(city['ed_idr_p1']+city['ed_idr_p2']+city['ed_idr_p3'])}</br>families:{int(city['fam_idr_p1']+city['fam_idr_p2']+city['fam_idr_p3'])}").add_to(m)
    m.add_child(Fullscreen())
    #save the map to the html file
    m.save(map_path)
    return open(map_path, 'r').read()

# Run the app
if __name__ == '__main__':
    app.run_server(debug=False, host='127.0.0.1', port=8054)