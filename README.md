# airflow-weather-pipeline

1. API 3.0 Switched to the 3.0 One Call "timemachine" endpoint. By using Airflow’s logical_date, the pipeline now pulls weather for the specific day the DAG is running.

```python
"dt": "{{ logical_date.int_timestamp }}"
```

2. Processing for 5 Cities

```python
CITIES = {
    "Lviv": (49.8397, 24.0297),
    "Kyiv": (50.4501, 30.5234),
    "Odesa": (46.4825, 30.7233),
    "Kharkiv": (49.9935, 36.2304),
    "Zhmerynka": (49.0345, 28.1062)
}

for city, coords in CITIES.items():
    extract_data = HttpOperator(
    )

    process_data = PythonOperator(
    )

    inject_data = SQLExecuteQueryOperator(
    )  
```

3. Expanded Database Schema: added more columns to the SQLite table: humidity, cloud coverage, wind speed, and also city column.

```python
INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed)
```

## Output
```sql
sqlite> SELECT * FROM measures ORDER BY timestamp DESC LIMIT 15;
+------------+-----------+------+----------+------------+------------+
| timestamp  |   city    | temp | humidity | cloudiness | wind_speed |
+------------+-----------+------+----------+------------+------------+
| 1774558954 | Kyiv      | 9.93 | 75.0     | 100.0      | 1.68       |
| 1774558954 | Lviv      | 9.11 | 78.0     | 100.0      | 2.28       |
| 1774558954 | Odesa     | 7.39 | 76.0     | 24.0       | 1.58       |
| 1774558954 | Kharkiv   | 8.48 | 91.0     | 62.0       | 2.41       |
| 1774558954 | Zhmerynka | 9.28 | 64.0     | 100.0      | 3.86       |
+------------+-----------+------+----------+------------+------------+
```


## Airflkow output
<img width="570" height="716" alt="image" src="https://github.com/user-attachments/assets/1c8335d7-39b6-458a-bc80-9fa554a8ac8d" />

<img width="1058" height="524" alt="image" src="https://github.com/user-attachments/assets/942505e4-a35b-414d-8784-38bcfadd7527" />
