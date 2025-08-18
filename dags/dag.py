from airflow.decorators import task
from airflow.decorators import dag
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from requests import get, auth
import requests
import json
import duckdb

columns_open_sky = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category"
]

url_all_states = "https://opensky-network.org/api/states/all?extended=true"
credentials = {
    "username": "romdou-api-client",
    "password": "yXv0RAo7N20Jk6V2CjkA0XqRKAOo5BFU"
}
DATA_FILE_PATH = "dags/data/data.json"
DATABASE = 'dags/data/bdd_airflow'

def to_dict(states_list,columns,timestamp):
    out = []
    for state in states_list:
        state_dict = dict(zip(columns, state ))
        state_dict["timestamp"] = timestamp
        out.append(state_dict)
    return out

@task()
def connect_to_api(url_all_states=url_all_states, credentials=credentials):
    # Logic to connect to the API goes here
    print("Connecting to API...")
    # Auth URL
    token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    
    # Get token
    token_response = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": credentials["username"],
        "client_secret": credentials["password"]
    })
    token_response.raise_for_status()  # Raise an error for bad responses
    access_token = token_response.json()["access_token"]
    print("Token retrieved successfully")
    
    # Call API
    headers = {"Authorization": f"Bearer {access_token}"}
    api_response = requests.get(url_all_states, headers=headers)
    api_response.raise_for_status()

    if api_response.status_code == 200:
        print("Data retrieved successfully")
        response = api_response.json()
        timestamp = response['time']
        states_list = response['states']
        return {"timestamp":timestamp, "states": states_list}
    else:
        print(f"Failed to retrieve data: {api_response.status_code} {api_response.text}")
        return False
    
@task()
def load_from_file(**kwargs):
    # Logic to load data from a file and write to DuckDB
    ti = kwargs['ti']
    data_file_name = ti.xcom_pull(task_ids='get_flight_data', key='filename')
    print(f"Loading data from file: {data_file_name}")
    con = None
    try:
        con = duckdb.connect(database=DATABASE)
        # Create table and insert data using DuckDB's JSON reader
        con.execute(f"""
            INSERT INTO bdd_airflow.main.openskynetwork_brute(
            SELECT * FROM '{data_file_name}')
        """)
        print("Data loaded into DuckDB successfully")
        con.close()
        return True
    except Exception as e:
        print(f"Error loading data into DuckDB: {e}")
        return None
    finally:
        if 'con' in locals():
            con.close()
    
@task(multiple_outputs=True)
def get_flight_data(data):
    # Logic to get data goes here
    print("Getting data...")
    if data:
        timestamp = data['timestamp']
        states = data['states']
        data_file_name = f'dags/data/data_{timestamp}.json'
        if states:
            states_json = to_dict(states, columns_open_sky, timestamp)
            with open(data_file_name, "w") as f:
                for state in states_json:
                    json.dump(state, f)
                    f.write("\n")
            return {"filename": data_file_name,
                    "nb_lines": len(states_json), 
                    "timestamp": timestamp}
        else:
            print("No states data available")
    else:
        print("No data available")
        
@task()
def check_row_numbers(ti=None):
    contenu_xcom = ti.xcom_pull(task_ids='get_flight_data', key='return_value')
    timestamp = contenu_xcom['timestamp']
    nb_lines_json = contenu_xcom['nb_lines']
    print("Checking row numbers...")
    # Logic to check row numbers goes here
    con = duckdb.connect(database=DATABASE,read_only=True)
    nb_lines_db = con.execute(f"SELECT COUNT(*) FROM bdd_airflow.main.openskynetwork_brute where timestamp = {timestamp}").fetchone()[0]
    print(f"Number of rows in table: {nb_lines_db}")
    con.close()
    if nb_lines_db != nb_lines_json:
        print("Nombre de lignes dans la base de données ne correspond pas au nombre de lignes dans le fichier JSON.")
        return False
    return True 

@task()
def check_duplicates():
    print("Checking for duplicates...")
    # Logic to check for duplicates goes here
    con = duckdb.connect(database=DATABASE,read_only=True)
    query = """
        SELECT callsign, time_position, last_contact, COUNT(*) as cnt
        FROM bdd_airflow.main.openskynetwork_brute
        GROUP BY callsign, time_position, last_contact
        HAVING cnt > 1
    """
    duplicates = con.execute(query).fetchall()
    if duplicates:
        print(f"Found {len(duplicates)} duplicate rows:")
        for row in duplicates:
            print(row)
    else:
        print("No duplicates found.")
    con.close()
    return True

@dag()
def flights_pipeline():
   (
       EmptyOperator(task_id="start")
        >> get_flight_data(connect_to_api())
        >> load_from_file()
        >> [check_row_numbers(), check_duplicates()]
        >> EmptyOperator(task_id="end")
   )
flights_pipeline_dag = flights_pipeline()

# This DAG is a simple example that starts with an EmptyOperator and ends with another EmptyOperator.
# It can be extended with more complex tasks as needed.

