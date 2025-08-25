import time
from airflow.decorators import task
from airflow.decorators import dag
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
import requests
import json
import duckdb

url_all_states = "https://opensky-network.org/api/states/all?extended=true"
CREDENTIALS = {
    "username": "romdou-api-client",
    "password": "yXv0RAo7N20Jk6V2CjkA0XqRKAOo5BFU"
}
DATA_FILE_PATH = "dags/data/data.json"
DATABASE = 'dags/data/bdd_airflow'
endpoint_to_params = {
    "states": {
        "url" : "https://opensky-network.org/api/states/all?extended=true",
        "columns" : [
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
            "category"],
        "target_table": "bdd_airflow.main.openskynetwork_brute",
        "timestamp_required": False
    },
    "flights": {
        "url": "https://opensky-network.org/api/flights/all/?begin={begin}&end={end}",
        "columns": [
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
            "callsign",
            "estDepartureAirportHorizDistance",
            "estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance",
            "estArrivalAirportVertDistance",
            "departureAirportCandidatesCount",
            "arrivalAirportCandidatesCount"
        ],
        "target_table": "bdd_airflow.main.flightsInTimeInterval",
        "timestamp_required": True
    }
}

def states_to_dict(states_list,columns,timestamp):
    out = []
    for state in states_list:
        state_dict = dict(zip(columns, state ))
        state_dict["timestamp"] = timestamp
        out.append(state_dict)
    return out

def flights_to_dict(flights_list, timestamp):
    out = []
    for flight in flights_list:
        flight["timestamp"] = timestamp
        out.append(flight)
    return out

@task(multiple_outputs=True)
def run_parameters(params=None):
    out = endpoint_to_params[params["endpoint"]]
    
    if out["timestamp_required"]:
        end_time = int(time.time())
        begin_time = end_time - 3600
        out["url"] = out["url"].format(begin=begin_time, end=end_time)
        
    return out

  
    
def load_from_file():
    return SQLExecuteQueryOperator(
        task_id="load_from_file",
        conn_id="DUCK_DB",
        sql="load_from_file.sql",
        return_last=True,
        show_return_value_in_logs=True
    )

@task(multiple_outputs=True)
def get_data(creds,ti=None):
    url = ti.xcom_pull(task_ids='run_parameters', key='url')
    colonnes = ti.xcom_pull(task_ids='run_parameters', key='columns')
    # Logic to connect to the API goes here
    print("Connecting to API...")
    # Auth URL
    token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    
    # Get token
    token_response = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": creds["username"],
        "client_secret": creds["password"]
    })
    token_response.raise_for_status()  # Raise an error for bad responses
    access_token = token_response.json()["access_token"]
    print("Token retrieved successfully")
    
    # Call API
    headers = {"Authorization": f"Bearer {access_token}"}

    api_response = requests.get(url, headers=headers)
    api_response.raise_for_status()

    if api_response.status_code == 200:
        print("Data retrieved successfully")
        print("Getting data...")
        response = api_response.json()
        if "states" in response:
            timestamp = response['time']
            results_json = states_to_dict(response['states'], colonnes, timestamp)
        else:
            timestamp = int(time.time())
            results_json = flights_to_dict(response, timestamp)

        data_file_name = f'dags/data/data_{timestamp}.json'
        
        with open(data_file_name, "w") as f:
                for state in results_json:
                    json.dump(state, f)
                    f.write("\n")
        return {"filename": data_file_name,
                "nb_lines": len(results_json), 
                "timestamp": timestamp}
    else:
        print(f"Failed to retrieve data: {api_response.status_code} {api_response.text}")
        return False

        
@task()        
def check_row_numbers(ti=None):
    nb_lines_json = ti.xcom_pull(task_ids='load_from_file', key='return_value')[0][0]
    nb_lines_db = ti.xcom_pull(task_ids='get_data', key='nb_lines')
    print("Checking row numbers...")
    if nb_lines_db != nb_lines_json:
        raise Exception("Nombre de lignes dans la base de données ne correspond pas au nombre de lignes dans le fichier JSON.")
    return True

def check_duplicates():
    print("Checking for duplicates...")
    return SQLExecuteQueryOperator(
        task_id="check_duplicates",
        conn_id="DUCK_DB",
        sql= "data/check_duplicates.sql",
        return_last=True,
        show_return_value_in_logs=True
    )

@dag(
    params={
        "endpoint": Param(
            default="states",
            enum=list(endpoint_to_params.keys()))
    }
)
def flights_pipeline():
   (
       EmptyOperator(task_id="start")
        >> run_parameters()
        >> get_data(creds=CREDENTIALS)
        >> load_from_file()
        >> [check_row_numbers(), check_duplicates()]
        >> EmptyOperator(task_id="end")
   )
flights_pipeline_dag = flights_pipeline()

# This DAG is a simple example that starts with an EmptyOperator and ends with another EmptyOperator.
# It can be extended with more complex tasks as needed.

