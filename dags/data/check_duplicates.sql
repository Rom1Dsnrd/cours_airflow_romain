SELECT callsign, time_position, last_contact, COUNT(*) as cnt
    FROM bdd_airflow.main.openskynetwork_brute
    GROUP BY callsign, time_position, last_contact
    HAVING cnt > 1