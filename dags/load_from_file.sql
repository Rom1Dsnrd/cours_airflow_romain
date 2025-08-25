INSERT INTO {{ti.xcom_pull(task_ids='run_parameters', key='target_table')}} (
    SELECT * FROM '{{ ti.xcom_pull(task_ids='get_data', key='filename')}}')