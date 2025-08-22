import mysql.connector
from mysql.connector import Error
import smtplib
from twilio.rest import Client
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import math
from tensorflow.keras.models import load_model
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd
from collections import Counter
from sklearn.preprocessing import LabelEncoder
import warnings
import random
import json

# Function to connect to MySQL database
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='sensor_data',
            user='root',
            password=''
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# MySQL database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'sensor_data'
}

# Twilio API configuration
twilio_account_sid = 'ACc30d65627b9c718220247985acc7f18a'
twilio_auth_token = '61de473d58d6e5edb0390840d163a6b6'
twilio_phone_number = '+19712385202'
recipient_phone_number = '+919344970465'


# Function to fetch the latest value from nrw column
def fetch_latest_nrw(connection):
    try:
        cursor = connection.cursor()
        query = "SELECT nrw FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to fetch latitude and longitude for sensor2
def fetch_sensor2_location(connection):
    try:
        cursor = connection.cursor()
        query = "SELECT FORMAT(latitude, 10) AS latitude, FORMAT(longitude, 10) AS longitude FROM sensors WHERE sensor = 'sensor 2' LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result
        else:
            return None
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to send an email
def send_email(value, latitude, longitude):
    # Your email configuration
    sender_email = "overvoidlurk1@gmail.com"
    receiver_email = "rlogasanjeev@gmail.com"
    password = "duij ugpf qilu fjqn"

    # Email content
    subject = "Alert: NRW Value Exceeded 1 Liter"
    body = f"The NRW value is {value}.Sensor's Latitude: {latitude}, Sensor's Longitude: {longitude}. Visit this site for more information http://localhost/Map/index.html"

    # Setting up the message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    # Connecting to the email server and sending the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            text = message.as_string()
            server.sendmail(sender_email, receiver_email, text)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")


# Function to check the database and send SMS if the condition is met
def check_and_send_sms():
    try:
        while True:
            # Connect to the MySQL database
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            # Query to retrieve the latest non_revenue_water record
            query = "SELECT timestamp, nrw, flowrate_diff FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1;"
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                timestamp, nrw_value, flowrate_diff = result
                current_time = datetime.now()
                time_difference = current_time - timestamp

                # Check if the condition is met (NRW value > 0.1 within the last minute)
                if nrw_value > 0.1 and time_difference < timedelta(minutes=1):
                    # Fetch latitude and longitude for sensor2
                    sensor2_location = fetch_sensor2_location(connection)

                    if sensor2_location:
                        latitude, longitude = sensor2_location
                        # Send SMS using Twilio
                        client = Client(twilio_account_sid, twilio_auth_token)
                        message = client.messages.create(
                            body=f"High NRW alert: NRW value is {nrw_value}, Flowrate Diff: {flowrate_diff},Sensor's Latitude: {latitude}, Sensor's Longitude: {longitude}, at {timestamp}",
                            from_=twilio_phone_number,
                            to=recipient_phone_number
                        )
                        print(f"SMS sent: {message.sid}")

                        # Stop checking after sending SMS
                        break

            # Close the database connection
            cursor.close()
            connection.close()

            # Wait for some time before checking again (e.g., 1 minute)
            time.sleep(1)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Connect to MySQL
    db_connection = connect_to_mysql()

    if db_connection:
        try:
            # Create cursor instances
            cursor_leak = db_connection.cursor()
            cursor_no_leak = db_connection.cursor()
            cursor_sensor = db_connection.cursor()
            cursor_zero_nrw = db_connection.cursor()
            cursor_sensors = db_connection.cursor()
            cursor_hydro = db_connection.cursor()

            # Create tables if not exists
            create_table_query_leak = """
                CREATE TABLE IF NOT EXISTS randomdata_sensor_leak (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    flow_rate FLOAT,
                    velocity FLOAT,
                    acceleration FLOAT,
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
                    velocity FLOAT,
                    acceleration FLOAT,
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
                    velocity FLOAT,
                    acceleration FLOAT,
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

            create_table_query_zero_nrw = """
                CREATE TABLE IF NOT EXISTS zero_non_revenue_water (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    nrw FLOAT,
                    flowrate_diff FLOAT
                )
            """
            cursor_zero_nrw.execute(create_table_query_zero_nrw)

            create_table_query_sensors = """
                CREATE TABLE IF NOT EXISTS sensors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    sensor VARCHAR(20),
                    latitude DECIMAL(15, 10),
                    longitude DECIMAL(15, 10),
                    flowrate_diff FLOAT,
                    nrw FLOAT
                )
            """
            cursor_sensors.execute(create_table_query_sensors)

            # Create a table to store hydrophone data if it doesn't exist
            create_table_query_hydro = '''
                CREATE TABLE IF NOT EXISTS hydrophone_data_table(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    value DOUBLE
                )
            '''
            cursor_hydro.execute(create_table_query_hydro)

            # Initialize rows with constant values
            initial_data_sensor1 = ('sensor 1', 28.63228980247208, 77.21009627490606, 0, 0)
            initial_data_sensor2 = ('sensor 2', 28.632332178240738, 77.20955446871794, 0, 0)

            # Insert initial data into sensors table
            insert_initial_data_query = """
                INSERT INTO sensors (sensor, latitude, longitude, flowrate_diff, nrw)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                flowrate_diff = VALUES(flowrate_diff),
                nrw = VALUES(nrw)
            """
            cursor_sensors.execute(insert_initial_data_query, initial_data_sensor1)
            cursor_sensors.execute(insert_initial_data_query, initial_data_sensor2)
            db_connection.commit()

            # Constants
            diameter_inches = 6
            diameter_meters = diameter_inches * 0.0254  # Convert inches to meters
            frequency = 1500  # Frequency in Hertz
            speed_of_sound = 1500  # Speed of sound in water in meters per second

            # Generate time values
            sampling_rate = 600  # Samples per second
            duration = 1  # Duration in seconds
            time_values = np.linspace(0, duration, int(sampling_rate * duration), endpoint=False)

            # Generate a sine wave with a frequency corresponding to one cycle per meter
            sine_wave = np.sin(2 * np.pi * frequency * time_values)

            # Normalize the values to fit within the typical range of hydrophone data
            sine_wave_normalized = sine_wave / np.max(np.abs(sine_wave))

            # Simulate hydrophone data for the pipe
            hydrophone_data = sine_wave_normalized

            # Simulate a leak in the pipeline
            pipe_length = 548  # Length of the pipe in meters
            leak_location = 400  # Location of the leak in meters

            # Create an array to simulate the leak effect
            leak_effect = np.ones_like(hydrophone_data)
            leak_start = int(len(hydrophone_data) * (leak_location / pipe_length))
            leak_end = leak_start + int(len(hydrophone_data) * 0.1)  # Assuming the leak affects 10% of the pipe
            leak_effect[leak_start:leak_end] *= 0.5  # Simulate a drop in amplitude for the leak
            

            # Define the output file path
            output_file_path = 'C:/wamp64/www/Dashboard/pin_point.txt'

            # Write the result to a text file
            with open(output_file_path, 'w') as file:
                r = "Approximate location of leak is in between  " + str(leak_start) + "  and  " + str(leak_end);
                file.write(r)

            # Apply the leak effect only at the leak location
            hydrophone_data *= leak_effect

            # Convert hydrophone_data to a list of tuples
            values_to_insert = [(value,) for value in hydrophone_data]

            # Insert hydrophone data into the database
            insert_query = 'INSERT INTO hydrophone_data_table (value) VALUES (%s)'
            cursor_hydro.executemany(insert_query, values_to_insert)
            db_connection.commit()


            # Data generation code (similar to the first code)
            total_water_passed_leak = 0
            total_water_passed_no_leak = 10  # Initialize to a value greater than the leak
            total_nrw = 0
            pipe_diameter = 152.4  # in mm
            # Convert pipe diameter to meters
            pipe_diameter_meters = pipe_diameter / 1000
            previous_velocity_leak = None
            previous_velocity_no_leak = None

            # Flag to indicate if NRW monitoring should continue
            continue_monitoring = True

            while continue_monitoring:
                
                base_flow_rate_gpm_leak = 10.0
                flow_rate_variation_leak = random.uniform(-0.1, 0.1)
                flow_rate_gpm_leak = base_flow_rate_gpm_leak + flow_rate_variation_leak
                flow_rate_gpm_leak = max(flow_rate_gpm_leak, 0)
                flow_rate_lps_leak = flow_rate_gpm_leak * 0.06309 / 10  # Divide by 10

                # Calculate velocity
                velocity_leak = (flow_rate_lps_leak ) / ((math.pi * pipe_diameter_meters**2)/4)

                # Calculate acceleration for leak data
                acceleration_leak = (velocity_leak - previous_velocity_leak) / time_interval if previous_velocity_leak is not None else 0

                previous_velocity_leak = velocity_leak

                base_pressure_leak = 60.0
                pressure_variation_leak = random.uniform(-1.0, 1.0)
                pressure_leak = (base_pressure_leak + pressure_variation_leak) * (flow_rate_gpm_leak / base_flow_rate_gpm_leak)  # Adjust pressure based on flow rate
                pressure_leak = max(pressure_leak, 0)
                time_interval = 1
                amount_passed_liter_leak = flow_rate_lps_leak * time_interval
                total_water_passed_leak += amount_passed_liter_leak
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f'Timestamp: {timestamp}, Flow Rate (Leak): {flow_rate_lps_leak:.4f} L/s, Amount Passed (Leak): {amount_passed_liter_leak:.4f} liters, Total Water Passed (Leak): {total_water_passed_leak:.4f} liters, Velocity (Leak): {velocity_leak:.4f} m/s, Acceleration (Leak): {acceleration_leak:.4f} m/s^2')

                
                insert_query_leak = "INSERT INTO randomdata_sensor_leak(timestamp, flow_rate,  amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
                insert_data_leak = (timestamp, flow_rate_lps_leak,  amount_passed_liter_leak, total_water_passed_leak, velocity_leak, acceleration_leak)
                cursor_leak.execute(insert_query_leak, insert_data_leak)
                db_connection.commit()

                base_flow_rate_gpm_no_leak = 12.0
                flow_rate_variation_no_leak = random.uniform(-0.1, 0.1)
                flow_rate_gpm_no_leak = base_flow_rate_gpm_no_leak + flow_rate_variation_no_leak
                flow_rate_gpm_no_leak = max(flow_rate_gpm_no_leak, 0)
                flow_rate_lps_no_leak = flow_rate_gpm_no_leak * 0.06309 / 10  # Divide by 10

                # Calculate velocity
                velocity_no_leak = (flow_rate_lps_no_leak ) / ((math.pi * pipe_diameter_meters**2)/4)

                 # Calculate acceleration for no-leak data
                acceleration_no_leak = (velocity_no_leak - previous_velocity_no_leak) / time_interval if previous_velocity_no_leak is not None else 0

                previous_velocity_no_leak = velocity_no_leak
                
                base_pressure_no_leak = 60.0
                pressure_variation_no_leak = random.uniform(-1.0, 1.0)
                pressure_no_leak = (base_pressure_no_leak + pressure_variation_no_leak) * (flow_rate_gpm_no_leak / base_flow_rate_gpm_no_leak)  # Adjust pressure based on flow rate
                pressure_no_leak = max(pressure_no_leak, 0)
                amount_passed_liter_no_leak = flow_rate_lps_no_leak * time_interval
                total_water_passed_no_leak += amount_passed_liter_no_leak
                print(f'Timestamp: {timestamp}, Flow Rate (No Leak): {flow_rate_lps_no_leak:.4f} L/s,  Amount Passed (No Leak): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (No Leak): {total_water_passed_no_leak:.4f} liters, Velocity (No Leak): {velocity_no_leak:.4f} m/s, Acceleration (No Leak): {acceleration_no_leak:.4f} m/s^2')


                insert_query_no_leak = "INSERT INTO RandomData_Sensor_NoLeak(timestamp, flow_rate,  amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
                insert_data_no_leak = (timestamp, flow_rate_lps_no_leak, amount_passed_liter_no_leak, total_water_passed_no_leak, velocity_no_leak, acceleration_no_leak)
                cursor_no_leak.execute(insert_query_no_leak, insert_data_no_leak)
                db_connection.commit()

                print(f'Timestamp: {timestamp}, Flow Rate (Sensor): {flow_rate_lps_no_leak:.4f} L/s,  Amount Passed (Sensor): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (Sensor): {total_water_passed_no_leak +2:.4f} liters, Velocity (Sensor): {velocity_no_leak:.4f} m/s, Acceleration (Sensor): {acceleration_no_leak:.4f} m/s^2')
                insert_query_sensor = "INSERT INTO RandomData_Sensor(timestamp, flow_rate,  amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
                insert_data_sensor = (timestamp, flow_rate_lps_no_leak, amount_passed_liter_no_leak, total_water_passed_no_leak + 10, velocity_no_leak, acceleration_no_leak)
                cursor_sensor.execute(insert_query_sensor, insert_data_sensor)
                db_connection.commit()

                # Fetch data from randomdata_sensor_leak
                cursor_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1")
                data_leak = cursor_leak.fetchone()

                # Fetch data from randomdata_sensor_no_leak
                cursor_no_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM RandomData_Sensor_NoLeak ORDER BY timestamp DESC LIMIT 1")
                data_no_leak = cursor_no_leak.fetchone()

                # Fetch data from randomdata_sensor
                cursor_sensor.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor ORDER BY timestamp DESC LIMIT 1")
                data_sensor = cursor_sensor.fetchone()

                if data_leak is not None and data_no_leak is not None and data_sensor is not None:
                    # Calculate differences
                    nrw_diff = data_no_leak[2] - data_leak[2] - 10
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

                    # Calculate differences for zero_non_revenue_water
                    nrw_diff_zero = int(data_sensor[2] - data_no_leak[2] - 10)
                    flowrate_diff_zero = data_sensor[1] - data_no_leak[1]

                    # Insert data into zero_non_revenue_water
                    insert_query_zero_nrw = """
                        INSERT INTO zero_non_revenue_water (timestamp, nrw, flowrate_diff)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE nrw=%s, flowrate_diff=%s
                    """
                    insert_data_zero_nrw = (data_no_leak[0], nrw_diff_zero, flowrate_diff_zero, nrw_diff_zero, flowrate_diff_zero)
                    cursor_zero_nrw.execute(insert_query_zero_nrw, insert_data_zero_nrw)
                    db_connection.commit()

                # Fetch data from zero_non_revenue_water
                cursor_zero_nrw.execute("SELECT timestamp, nrw, flowrate_diff FROM zero_non_revenue_water ORDER BY timestamp DESC LIMIT 1")
                data_zero_nrw = cursor_zero_nrw.fetchone()

                # Fetch data from non_revenue_water
                cursor_leak.execute("SELECT timestamp, nrw, flowrate_diff FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1")
                data_nrw = cursor_leak.fetchone()

                if data_zero_nrw is not None and data_nrw is not None:
                    # Update values for sensor 1
                    update_query_sensor1 = """
                        UPDATE sensors
                        SET timestamp = %s,
                            flowrate_diff = %s,
                            nrw = %s
                        WHERE sensor = 'sensor 1'
                    """
                    update_data_sensor1 = (data_zero_nrw[0], data_zero_nrw[2], data_zero_nrw[1])
                    cursor_sensors.execute(update_query_sensor1, update_data_sensor1)
                    db_connection.commit()

                    # Update values for sensor 2
                    update_query_sensor2 = """
                        UPDATE sensors
                        SET timestamp = %s,
                            flowrate_diff = %s,
                            nrw = %s
                        WHERE sensor = 'sensor 2'
                    """
                    update_data_sensor2 = (data_nrw[0], data_nrw[2], data_nrw[1])
                    cursor_sensors.execute(update_query_sensor2, update_data_sensor2)
                    db_connection.commit()

                # NRW monitoring and email sending code (similar to the second code)
                latest_nrw = fetch_latest_nrw(db_connection)

                if latest_nrw is not None:
                    print(f"Latest NRW value: {latest_nrw}")

                    if latest_nrw > 0.1:
                        # Fetch latitude and longitude for sensor 2
                        sensor2_location = fetch_sensor2_location(db_connection)

                        if sensor2_location:
                            latitude, longitude = sensor2_location

                            # Call send_email with the necessary parameters
                            send_email(latest_nrw, latitude, longitude)

                            # Stop monitoring NRW and sending emails
                            continue_monitoring = False


                time.sleep(1)

        except KeyboardInterrupt:
            print("Program ended by user.")
    
def execute_second_code():
    # Load the model from the saved file
    loaded_model = load_model('Leak_Prediction.keras')

    # Connect to MySQL database
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="sensor_data"
    )

    # Function to fetch data from MySQL
    def fetch_data(table_name, columns):
        cursor = db_connection.cursor()
        query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT 60;"
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data, columns=columns)
        return df

    # Define columns for acceleration data
    columns_noleak = ['acceleration']
    columns_leak = ['acceleration']

    # Fetch data from MySQL for 'randomdata_sensor_noleak' and 'randomdata_sensor_leak' tables
    data_noleak = fetch_data('randomdata_sensor_noleak', columns_noleak)
    data_leak = fetch_data('randomdata_sensor_leak', columns_leak)

    new_data = pd.concat([data_noleak, data_leak], axis=1)

    # Load the pre-trained model
    loaded_model = load_model('Leak_Prediction.keras')

    # Preprocess the new data (scaling)
    scaler = StandardScaler()
    new_data_scaled = scaler.fit_transform(new_data)

    # Make predictions
    predictions = loaded_model.predict(new_data_scaled)

    # Get the predicted class with the highest probability for each sample
    predicted_labels = predictions.argmax(axis=1)

    # creating counter object
    count = Counter(predicted_labels)

    # maximum number of occurrences
    max_count = max(count.most_common(), key=lambda x: x[1])

    if max_count[0] == 0:
        result= 'Circumferential Crack'
    elif max_count[0] == 1:
        result= "Gasket Leak"
    elif max_count[0] == 2:
        result= 'Longitudinal Crack'
    elif max_count[0] == 3:
        result= 'Non Leak'
    else:
        result= "Orifice Leak"

    print(result)
    
    # Define the output file path
    output_file_path = 'C:/wamp64/www/Dashboard/leak_type.txt'

    # Write the result to a text file
    with open(output_file_path, 'w') as file:
        file.write(result)

    return result

def theft():
    result= "Theft"
    print(result)

    # Define the output file path
    output_file_path = 'C:/wamp64/www/Dashboard/leak_type.txt'

    # Write the result to a text file
    with open(output_file_path, 'w') as file:
        file.write(result)

# Data generation loop continues even after NRW monitoring stops
while True:
    base_flow_rate_gpm_leak = 10.0
    flow_rate_variation_leak = random.uniform(-0.1, 0.1)
    flow_rate_gpm_leak = base_flow_rate_gpm_leak + flow_rate_variation_leak
    flow_rate_gpm_leak = max(flow_rate_gpm_leak, 0)
    flow_rate_lps_leak = flow_rate_gpm_leak * 0.06309 / 10  # Divide by 10

    # Calculate velocity
    velocity_leak = (flow_rate_lps_leak ) / ((math.pi * pipe_diameter_meters**2)/4)

    # Calculate acceleration for leak data
    acceleration_leak = (velocity_leak - previous_velocity_leak) / time_interval if previous_velocity_leak is not None else 0

    previous_velocity_leak = velocity_leak

    base_pressure_leak = 60.0
    pressure_variation_leak = random.uniform(-1.0, 1.0)
    pressure_leak = (base_pressure_leak + pressure_variation_leak) * (flow_rate_gpm_leak / base_flow_rate_gpm_leak)  # Adjust pressure based on flow rate
    pressure_leak = max(pressure_leak, 0)
    time_interval = 1
    amount_passed_liter_leak = flow_rate_lps_leak * time_interval
    total_water_passed_leak += amount_passed_liter_leak
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f'Timestamp: {timestamp}, Flow Rate (Leak): {flow_rate_lps_leak:.4f} L/s, Amount Passed (Leak): {amount_passed_liter_leak:.4f} liters, Total Water Passed (Leak): {total_water_passed_leak:.4f} liters, Velocity (Leak): {velocity_leak:.4f} m/s, Acceleration (Leak): {acceleration_leak:.4f} m/s^2')

        
    insert_query_leak = "INSERT INTO randomdata_sensor_leak(timestamp, flow_rate, amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
    insert_data_leak = (timestamp, flow_rate_lps_leak,  amount_passed_liter_leak, total_water_passed_leak, velocity_leak, acceleration_leak)
    cursor_leak.execute(insert_query_leak, insert_data_leak)
    db_connection.commit()

    base_flow_rate_gpm_no_leak = 12.0
    flow_rate_variation_no_leak = random.uniform(-0.1, 0.1)
    flow_rate_gpm_no_leak = base_flow_rate_gpm_no_leak + flow_rate_variation_no_leak
    flow_rate_gpm_no_leak = max(flow_rate_gpm_no_leak, 0)
    flow_rate_lps_no_leak = flow_rate_gpm_no_leak * 0.06309 / 10  # Divide by 10

    # Calculate velocity
    velocity_no_leak = (flow_rate_lps_no_leak ) / ((math.pi * pipe_diameter_meters**2)/4)

    # Calculate acceleration for no-leak data
    acceleration_no_leak = (velocity_no_leak - previous_velocity_no_leak) / time_interval if previous_velocity_no_leak is not None else 0

    previous_velocity_no_leak = velocity_no_leak
        
    base_pressure_no_leak = 60.0
    pressure_variation_no_leak = random.uniform(-1.0, 1.0)
    pressure_no_leak = (base_pressure_no_leak + pressure_variation_no_leak) * (flow_rate_gpm_no_leak / base_flow_rate_gpm_no_leak)  # Adjust pressure based on flow rate
    pressure_no_leak = max(pressure_no_leak, 0)
    amount_passed_liter_no_leak = flow_rate_lps_no_leak * time_interval
    total_water_passed_no_leak += amount_passed_liter_no_leak
    print(f'Timestamp: {timestamp}, Flow Rate (No Leak): {flow_rate_lps_no_leak:.4f} L/s,  Amount Passed (No Leak): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (No Leak): {total_water_passed_no_leak:.4f} liters, Velocity (No Leak): {velocity_no_leak:.4f} m/s, Acceleration (No Leak): {acceleration_no_leak:.4f} m/s^2')


    insert_query_no_leak = "INSERT INTO RandomData_Sensor_NoLeak(timestamp, flow_rate,  amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
    insert_data_no_leak = (timestamp, flow_rate_lps_no_leak,  amount_passed_liter_no_leak, total_water_passed_no_leak, velocity_no_leak, acceleration_no_leak)
    cursor_no_leak.execute(insert_query_no_leak, insert_data_no_leak)
    db_connection.commit()

    print(f'Timestamp: {timestamp}, Flow Rate (Sensor): {flow_rate_lps_no_leak:.4f} L/s, Amount Passed (Sensor): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (Sensor): {total_water_passed_no_leak +2:.4f} liters, Velocity (Sensor): {velocity_no_leak:.4f} m/s, Acceleration (Sensor): {acceleration_no_leak:.4f} m/s^2')
    insert_query_sensor = "INSERT INTO RandomData_Sensor(timestamp, flow_rate, amount_passed, total_water_passed, velocity, acceleration) VALUES ( %s, %s, %s, %s, %s, %s)"
    insert_data_sensor = (timestamp, flow_rate_lps_no_leak, amount_passed_liter_no_leak, total_water_passed_no_leak + 10, velocity_no_leak, acceleration_no_leak)
    cursor_sensor.execute(insert_query_sensor, insert_data_sensor)
    db_connection.commit()

    # Fetch data from randomdata_sensor_leak
    cursor_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1")
    data_leak = cursor_leak.fetchone()

    # Fetch data from randomdata_sensor_no_leak
    cursor_no_leak.execute("SELECT timestamp, flow_rate, total_water_passed FROM RandomData_Sensor_NoLeak ORDER BY timestamp DESC LIMIT 1")
    data_no_leak = cursor_no_leak.fetchone()

    # Fetch data from randomdata_sensor
    cursor_sensor.execute("SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor ORDER BY timestamp DESC LIMIT 1")
    data_sensor = cursor_sensor.fetchone()

    if data_leak is not None and data_no_leak is not None and data_sensor is not None:
        # Calculate differences
        nrw_diff = data_no_leak[2] - data_leak[2] - 10
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

        # Calculate differences for zero_non_revenue_water
        nrw_diff_zero = int(data_sensor[2] - data_no_leak[2] - 10)
        flowrate_diff_zero = data_sensor[1] - data_no_leak[1]

        # Insert data into zero_non_revenue_water
        insert_query_zero_nrw = """
                INSERT INTO zero_non_revenue_water (timestamp, nrw, flowrate_diff)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE nrw=%s, flowrate_diff=%s
            """
        insert_data_zero_nrw = (data_no_leak[0], nrw_diff_zero, flowrate_diff_zero, nrw_diff_zero, flowrate_diff_zero)
        cursor_zero_nrw.execute(insert_query_zero_nrw, insert_data_zero_nrw)
        db_connection.commit()

    # Fetch data from zero_non_revenue_water
    cursor_zero_nrw.execute("SELECT timestamp, nrw, flowrate_diff FROM zero_non_revenue_water ORDER BY timestamp DESC LIMIT 1")
    data_zero_nrw = cursor_zero_nrw.fetchone()

    # Fetch data from non_revenue_water
    cursor_leak.execute("SELECT timestamp, nrw, flowrate_diff FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1")
    data_nrw = cursor_leak.fetchone()

    if data_zero_nrw is not None and data_nrw is not None:
        # Update values for sensor 1
        update_query_sensor1 = """
                UPDATE sensors
                SET timestamp = %s,
                    flowrate_diff = %s,
                    nrw = %s
                WHERE sensor = 'sensor 1'
            """
        update_data_sensor1 = (data_zero_nrw[0], data_zero_nrw[2], data_zero_nrw[1])
        cursor_sensors.execute(update_query_sensor1, update_data_sensor1)
        db_connection.commit()

        # Update values for sensor 2
        update_query_sensor2 = """
                UPDATE sensors
                SET timestamp = %s,
                    flowrate_diff = %s,
                    nrw = %s
                WHERE sensor = 'sensor 2'
            """
        update_data_sensor2 = (data_nrw[0], data_nrw[2], data_nrw[1])
        cursor_sensors.execute(update_query_sensor2, update_data_sensor2)
        db_connection.commit()

        cursor_sensor.execute("SELECT id FROM randomdata_sensor ORDER BY id DESC LIMIT 1")
        current_id = cursor_sensor.fetchone()[0]

        cursor_no_leak.execute("SELECT flow_rate FROM randomdata_sensor_noleak ORDER BY id DESC LIMIT 1")
        current_no_leak = cursor_no_leak.fetchone()[0]

        cursor_leak.execute("SELECT flow_rate FROM randomdata_sensor_leak ORDER BY id DESC LIMIT 1")
        current_leak = cursor_leak.fetchone()[0]

        if(current_no_leak *0.5 > current_leak):
            theft()
        elif current_id == 60:
            check_and_send_sms()
            
        
    time.sleep(1)   
