import mysql.connector
import time

# Replace the placeholders with your actual MySQL server details
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sanju2907",
    database="sensor_data"
)

cursor_no_leak = db_connection.cursor()
cursor_leak = db_connection.cursor()

# Create non_revenue_water table if not exists
create_table_query = """
    CREATE TABLE IF NOT EXISTS zero_non_revenue_water (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        nrw FLOAT,
        flowrate_diff FLOAT
    )
"""
cursor_no_leak.execute(create_table_query)

def generate_and_insert_combined_data():
    total_nrw = 0

    while True:
        # Fetch data from randomdata_sensor_no_leak
        cursor_no_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_no_leak ORDER BY timestamp DESC LIMIT 1")
        data_no_leak = cursor_no_leak.fetchone()

        # Fetch data from randomdata_sensor
        cursor_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor ORDER BY timestamp DESC LIMIT 1")
        data_sensor = cursor_leak.fetchone()

        if data_no_leak is not None and data_sensor is not None:
            # Calculate differences
            nrw_diff = data_sensor[2] - data_no_leak[2]
            flowrate_diff = data_sensor[1] - data_no_leak[1]

            # Insert data into non_revenue_water
            insert_query = """
                INSERT INTO zero_non_revenue_water (timestamp, nrw, flowrate_diff)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE nrw=%s, flowrate_diff=%s
            """
            insert_data = (data_no_leak[0], nrw_diff, flowrate_diff, nrw_diff, flowrate_diff)
            cursor_no_leak.execute(insert_query, insert_data)
            db_connection.commit()

            # Update total_nrw
            total_nrw += nrw_diff

        time.sleep(1)

# Uncomment the line below to start generating and inserting combined realistic flow rate data
generate_and_insert_combined_data()
