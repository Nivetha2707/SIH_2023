import random
import time
import mysql.connector

# Establish a connection to your MySQL server
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sanju2907",
    database="Sensor_Data"
)

# Create a cursor object to interact with the database
cursor = db_connection.cursor()

def generate_and_insert_flowrate_data():
    total_water_passed = 0  # To accumulate the total amount of water passed through the sensor

    while True:
        # Simulate a highly stable and consistent flow rate in gallons per minute (GPM)
        base_flow_rate_gpm = 12.0  # Replace with the expected base flow rate for your water supply
        flow_rate_variation = random.uniform(-0.1, 0.1)  # Simulate very small changes
        flow_rate_gpm = base_flow_rate_gpm + flow_rate_variation

        # Ensure the flow rate doesn't go below 0
        flow_rate_gpm = max(flow_rate_gpm, 0)

        # Convert flow rate to liters per second (L/s)
        flow_rate_lps = flow_rate_gpm * 0.06309

        # Simulate a highly stable pressure within the water supply network
        base_pressure = 60.0  # Replace with the expected base pressure
        pressure_variation = random.uniform(-1.0, 1.0)  # Simulate very small changes
        pressure = base_pressure + pressure_variation

        # Ensure pressure doesn't go below 0
        pressure = max(pressure, 0)

        # Calculate the amount of water passed through the sensor in the last second
        time_interval = 1  # Assuming data is generated every 1 second
        amount_passed_liter = flow_rate_lps * time_interval  # The flow rate is now in L/s, so no need to divide by 60.0

        # Accumulate the total amount of water passed
        total_water_passed += amount_passed_liter

        # Simulate timestamp
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        # Print or store the generated data as needed
        print(f'Timestamp: {timestamp}, Flow Rate: {flow_rate_lps:.4f} L/s, Pressure: {pressure:.2f} PSI, Amount Passed: {amount_passed_liter:.4f} liters, Total Water Passed: {total_water_passed:.4f} liters')

        # Insert data into MySQL database
        insert_query = "INSERT INTO  RandomData_Sensor_NoLeak(timestamp, flow_rate, pressure, amount_passed, total_water_passed) VALUES (%s, %s, %s, %s, %s)"
        insert_data = (timestamp, flow_rate_lps, pressure, amount_passed_liter, total_water_passed)

        cursor.execute(insert_query, insert_data)
        db_connection.commit()

        # Adjust the sleep duration based on how often you want to generate data
        time.sleep(1)  # Sleep for 1 second

# Uncomment the line below to start generating and inserting realistic flow rate data
generate_and_insert_flowrate_data()
