import random
import time
import mysql.connector

# Replace the placeholders with your actual MySQL server details
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="sensor_data"
)

cursor_leak = db_connection.cursor()
cursor_no_leak = db_connection.cursor()
cursor_sensor = db_connection.cursor()

# Create tables if not exists
create_table_query_leak = """
    CREATE TABLE IF NOT EXISTS randomdata_sensor_leak (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        flow_rate FLOAT,
        pressure FLOAT,
        amount_passed FLOAT,
        total_water_passed FLOAT
    )
"""
cursor_leak.execute(create_table_query_leak)

create_table_query_no_leak = """
    CREATE TABLE IF NOT EXISTS RandomData_Sensor_NoLeak (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        flow_rate FLOAT,
        pressure FLOAT,
        amount_passed FLOAT,
        total_water_passed FLOAT
    )
"""
cursor_no_leak.execute(create_table_query_no_leak)

create_table_query_sensor = """
    CREATE TABLE IF NOT EXISTS RandomData_Sensor (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        flow_rate FLOAT,
        pressure FLOAT,
        amount_passed FLOAT,
        total_water_passed FLOAT
    )
"""
cursor_sensor.execute(create_table_query_sensor)

create_table_query_nrw = """
    CREATE TABLE IF NOT EXISTS non_revenue_water (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        nrw FLOAT,
        flowrate_diff FLOAT
    )
"""
cursor_leak.execute(create_table_query_nrw)

def generate_and_insert_combined_data():
    total_water_passed_leak = 0
    total_water_passed_no_leak = 2  # Initialize to a value greater than the leak
    total_nrw = 0

    while True:
        base_flow_rate_gpm_leak = 10.0
        flow_rate_variation_leak = random.uniform(-0.1, 0.1)
        flow_rate_gpm_leak = base_flow_rate_gpm_leak + flow_rate_variation_leak
        flow_rate_gpm_leak = max(flow_rate_gpm_leak, 0)
        flow_rate_lps_leak = flow_rate_gpm_leak * 0.06309
        base_pressure_leak = 60.0
        pressure_variation_leak = random.uniform(-1.0, 1.0)
        pressure_leak = base_pressure_leak + pressure_variation_leak
        pressure_leak = max(pressure_leak, 0)
        time_interval = 1
        amount_passed_liter_leak = flow_rate_lps_leak * time_interval
        total_water_passed_leak += amount_passed_liter_leak
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'Timestamp: {timestamp}, Flow Rate (Leak): {flow_rate_lps_leak:.4f} L/s, Pressure (Leak): {pressure_leak:.2f} PSI, Amount Passed (Leak): {amount_passed_liter_leak:.4f} liters, Total Water Passed (Leak): {total_water_passed_leak:.4f} liters')

        insert_query_leak = "INSERT INTO randomdata_sensor_leak(timestamp, flow_rate, pressure, amount_passed, total_water_passed) VALUES (%s, %s, %s, %s, %s)"
        insert_data_leak = (timestamp, flow_rate_lps_leak, pressure_leak, amount_passed_liter_leak, total_water_passed_leak)
        cursor_leak.execute(insert_query_leak, insert_data_leak)
        db_connection.commit()

        base_flow_rate_gpm_no_leak = 12.0
        flow_rate_variation_no_leak = random.uniform(-0.1, 0.1)
        flow_rate_gpm_no_leak = base_flow_rate_gpm_no_leak + flow_rate_variation_no_leak
        flow_rate_gpm_no_leak = max(flow_rate_gpm_no_leak, 0)
        flow_rate_lps_no_leak = flow_rate_gpm_no_leak * 0.06309
        base_pressure_no_leak = 60.0
        pressure_variation_no_leak = random.uniform(-1.0, 1.0)
        pressure_no_leak = base_pressure_no_leak + pressure_variation_no_leak
        pressure_no_leak = max(pressure_no_leak, 0)
        amount_passed_liter_no_leak = flow_rate_lps_no_leak * time_interval
        total_water_passed_no_leak += amount_passed_liter_no_leak
        print(f'Timestamp: {timestamp}, Flow Rate (No Leak): {flow_rate_lps_no_leak:.4f} L/s, Pressure (No Leak): {pressure_no_leak:.2f} PSI, Amount Passed (No Leak): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (No Leak): {total_water_passed_no_leak:.4f} liters')

        insert_query_no_leak = "INSERT INTO RandomData_Sensor_NoLeak(timestamp, flow_rate, pressure, amount_passed, total_water_passed) VALUES (%s, %s, %s, %s, %s)"
        insert_data_no_leak = (timestamp, flow_rate_lps_no_leak, pressure_no_leak, amount_passed_liter_no_leak, total_water_passed_no_leak)
        cursor_no_leak.execute(insert_query_no_leak, insert_data_no_leak)
        db_connection.commit()

        print(f'Timestamp: {timestamp}, Flow Rate (Sensor): {flow_rate_lps_no_leak:.4f} L/s, Pressure (Sensor): {pressure_no_leak:.2f} PSI, Amount Passed (Sensor): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (Sensor): {total_water_passed_no_leak +2:.4f} liters')
        insert_query_sensor = "INSERT INTO RandomData_Sensor(timestamp, flow_rate, pressure, amount_passed, total_water_passed) VALUES (%s, %s, %s, %s, %s)"
        insert_data_sensor = (timestamp, flow_rate_lps_no_leak, pressure_no_leak, amount_passed_liter_no_leak, total_water_passed_no_leak +2)
        cursor_sensor.execute(insert_query_sensor, insert_data_sensor)
        db_connection.commit()

        # Fetch data from randomdata_sensor_leak
        cursor_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1")
        data_leak = cursor_leak.fetchone()

        # Fetch data from randomdata_sensor_no_leak
        cursor_no_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM RandomData_Sensor_NoLeak ORDER BY timestamp DESC LIMIT 1")
        data_no_leak = cursor_no_leak.fetchone()

        if data_leak is not None and data_no_leak is not None:
            # Calculate differences
            nrw_diff = data_no_leak[2] - data_leak[2] - 2
            flowrate_diff = data_no_leak[1] - data_leak[1]

            # Insert data into non_revenue_water
            insert_query_nrw = """
                INSERT INTO non_revenue_water (timestamp, nrw, flowrate_diff)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE nrw=%s, flowrate_diff=%s
            """
            insert_data_nrw = (data_leak[0], nrw_diff, flowrate_diff, nrw_diff, flowrate_diff)
            cursor_leak.execute(insert_query_nrw, insert_data_nrw)
            db_connection.commit()

        time.sleep(1)

# Uncomment the line below to start generating and inserting combined realistic flow rate data
generate_and_insert_combined_data()
