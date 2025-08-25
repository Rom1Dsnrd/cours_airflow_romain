SELECT callsign, timestamp, COUNT(*) as cnt
    FROM {{ti.xcom_pull(task_ids='run_parameters', key='target_table')}}
    GROUP BY callsign, timestamp
    HAVING cnt > 1