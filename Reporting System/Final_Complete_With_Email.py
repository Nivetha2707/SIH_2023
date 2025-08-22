import mysql.connector
from mysql.connector import Error
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import random
import math


# Function to connect to MySQL database
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost", database="sensor_data", user="root", password=""
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None


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


# Function to send an email
def send_email(value):
    # Your email configuration
    sender_email = "overvoidlurk1@gmail.com"
    receiver_email = "rlogasanjeev@gmail.com"
    password = "duij ugpf"

    # Email content
    subject = "Alert: NRW Value Exceeded 0.1 Liter"
    body = f"The NRW value is {value} .Vist this site for more information http://localhost/Map/index.html"

    # Setting up the message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    # Connecting to the email server and sending the email
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, password)
            text = message.as_string()
            server.sendmail(sender_email, receiver_email, text)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")


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

            # Create tables if not exists
            create_table_query_leak = """
                CREATE TABLE IF NOT EXISTS randomdata_sensor_leak (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    flow_rate FLOAT,
                    velocity FLOAT,
                    acceleration FLOAT,
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
                    velocity FLOAT,
                    acceleration FLOAT,
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
                    velocity FLOAT,
                    acceleration FLOAT,
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

            # Initialize rows with constant values
            initial_data_sensor1 = ("sensor 1", 11.04902513715, 77.08163088769381, 0, 0)
            initial_data_sensor2 = ("sensor 2", 11.04747541146, 77.07984766258147, 0, 0)

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

            # Data generation code (similar to the first code)
            total_water_passed_leak = 0
            total_water_passed_no_leak = (
                10  # Initialize to a value greater than the leak
            )
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
                velocity_leak = (flow_rate_lps_leak) / (
                    (math.pi * pipe_diameter_meters**2) / 4
                )

                # Calculate acceleration for leak data
                acceleration_leak = (
                    (velocity_leak - previous_velocity_leak) / time_interval
                    if previous_velocity_leak is not None
                    else 0
                )

                previous_velocity_leak = velocity_leak

                base_pressure_leak = 60.0
                pressure_variation_leak = random.uniform(-1.0, 1.0)
                pressure_leak = (base_pressure_leak + pressure_variation_leak) * (
                    flow_rate_gpm_leak / base_flow_rate_gpm_leak
                )  # Adjust pressure based on flow rate
                pressure_leak = max(pressure_leak, 0)
                time_interval = 1
                amount_passed_liter_leak = flow_rate_lps_leak * time_interval
                total_water_passed_leak += amount_passed_liter_leak
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"Timestamp: {timestamp}, Flow Rate (Leak): {flow_rate_lps_leak:.4f} L/s, Pressure (Leak): {pressure_leak:.2f} PSI, Amount Passed (Leak): {amount_passed_liter_leak:.4f} liters, Total Water Passed (Leak): {total_water_passed_leak:.4f} liters, Velocity (Leak): {velocity_leak:.4f} m/s, Acceleration (Leak): {acceleration_leak:.4f} m/s^2"
                )

                insert_query_leak = "INSERT INTO randomdata_sensor_leak(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                insert_data_leak = (
                    timestamp,
                    flow_rate_lps_leak,
                    pressure_leak,
                    amount_passed_liter_leak,
                    total_water_passed_leak,
                    velocity_leak,
                    acceleration_leak,
                )
                cursor_leak.execute(insert_query_leak, insert_data_leak)
                db_connection.commit()

                base_flow_rate_gpm_no_leak = 12.0
                flow_rate_variation_no_leak = random.uniform(-0.1, 0.1)
                flow_rate_gpm_no_leak = (
                    base_flow_rate_gpm_no_leak + flow_rate_variation_no_leak
                )
                flow_rate_gpm_no_leak = max(flow_rate_gpm_no_leak, 0)
                flow_rate_lps_no_leak = (
                    flow_rate_gpm_no_leak * 0.06309 / 10
                )  # Divide by 10

                # Calculate velocity
                velocity_no_leak = (flow_rate_lps_no_leak) / (
                    (math.pi * pipe_diameter_meters**2) / 4
                )

                # Calculate acceleration for no-leak data
                acceleration_no_leak = (
                    (velocity_no_leak - previous_velocity_no_leak) / time_interval
                    if previous_velocity_no_leak is not None
                    else 0
                )

                previous_velocity_no_leak = velocity_no_leak

                base_pressure_no_leak = 60.0
                pressure_variation_no_leak = random.uniform(-1.0, 1.0)
                pressure_no_leak = (
                    base_pressure_no_leak + pressure_variation_no_leak
                ) * (
                    flow_rate_gpm_no_leak / base_flow_rate_gpm_no_leak
                )  # Adjust pressure based on flow rate
                pressure_no_leak = max(pressure_no_leak, 0)
                amount_passed_liter_no_leak = flow_rate_lps_no_leak * time_interval
                total_water_passed_no_leak += amount_passed_liter_no_leak
                print(
                    f"Timestamp: {timestamp}, Flow Rate (No Leak): {flow_rate_lps_no_leak:.4f} L/s, Pressure (No Leak): {pressure_no_leak:.2f} PSI, Amount Passed (No Leak): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (No Leak): {total_water_passed_no_leak:.4f} liters, Velocity (No Leak): {velocity_no_leak:.4f} m/s, Acceleration (No Leak): {acceleration_no_leak:.4f} m/s^2"
                )

                insert_query_no_leak = "INSERT INTO RandomData_Sensor_NoLeak(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                insert_data_no_leak = (
                    timestamp,
                    flow_rate_lps_no_leak,
                    pressure_no_leak,
                    amount_passed_liter_no_leak,
                    total_water_passed_no_leak,
                    velocity_no_leak,
                    acceleration_no_leak,
                )
                cursor_no_leak.execute(insert_query_no_leak, insert_data_no_leak)
                db_connection.commit()

                print(
                    f"Timestamp: {timestamp}, Flow Rate (Sensor): {flow_rate_lps_no_leak:.4f} L/s, Pressure (Sensor): {pressure_no_leak:.2f} PSI, Amount Passed (Sensor): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (Sensor): {total_water_passed_no_leak +2:.4f} liters, Velocity (Sensor): {velocity_no_leak:.4f} m/s, Acceleration (Sensor): {acceleration_no_leak:.4f} m/s^2"
                )
                insert_query_sensor = "INSERT INTO RandomData_Sensor(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                insert_data_sensor = (
                    timestamp,
                    flow_rate_lps_no_leak,
                    pressure_no_leak,
                    amount_passed_liter_no_leak,
                    total_water_passed_no_leak + 10,
                    velocity_no_leak,
                    acceleration_no_leak,
                )
                cursor_sensor.execute(insert_query_sensor, insert_data_sensor)
                db_connection.commit()

                # Fetch data from randomdata_sensor_leak
                cursor_leak.execute(
                    "SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1"
                )
                data_leak = cursor_leak.fetchone()

                # Fetch data from randomdata_sensor_no_leak
                cursor_no_leak.execute(
                    "SELECT timestamp, flow_rate, total_water_passed FROM RandomData_Sensor_NoLeak ORDER BY timestamp DESC LIMIT 1"
                )
                data_no_leak = cursor_no_leak.fetchone()

                # Fetch data from randomdata_sensor
                cursor_sensor.execute(
                    "SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor ORDER BY timestamp DESC LIMIT 1"
                )
                data_sensor = cursor_sensor.fetchone()

                if (
                    data_leak is not None
                    and data_no_leak is not None
                    and data_sensor is not None
                ):
                    # Calculate differences
                    nrw_diff = data_no_leak[2] - data_leak[2] - 10
                    flowrate_diff = data_no_leak[1] - data_leak[1]

                    # Insert data into non_revenue_water
                    insert_query_nrw = """
                        INSERT INTO non_revenue_water (timestamp, nrw, flowrate_diff)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE nrw=%s, flowrate_diff=%s
                    """
                    insert_data_nrw = (
                        data_leak[0],
                        nrw_diff,
                        flowrate_diff,
                        nrw_diff,
                        flowrate_diff,
                    )
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
                    insert_data_zero_nrw = (
                        data_no_leak[0],
                        nrw_diff_zero,
                        flowrate_diff_zero,
                        nrw_diff_zero,
                        flowrate_diff_zero,
                    )
                    cursor_zero_nrw.execute(insert_query_zero_nrw, insert_data_zero_nrw)
                    db_connection.commit()

                # Fetch data from zero_non_revenue_water
                cursor_zero_nrw.execute(
                    "SELECT timestamp, nrw, flowrate_diff FROM zero_non_revenue_water ORDER BY timestamp DESC LIMIT 1"
                )
                data_zero_nrw = cursor_zero_nrw.fetchone()

                # Fetch data from non_revenue_water
                cursor_leak.execute(
                    "SELECT timestamp, nrw, flowrate_diff FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1"
                )
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
                    update_data_sensor1 = (
                        data_zero_nrw[0],
                        data_zero_nrw[2],
                        data_zero_nrw[1],
                    )
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
                        send_email(latest_nrw)

                        # Stop monitoring NRW and sending emails
                        continue_monitoring = False

                time.sleep(1)

        except KeyboardInterrupt:
            print("Program ended by user.")

# Data generation loop continues even after NRW monitoring stops
while True:
    base_flow_rate_gpm_leak = 10.0
    flow_rate_variation_leak = random.uniform(-0.1, 0.1)
    flow_rate_gpm_leak = base_flow_rate_gpm_leak + flow_rate_variation_leak
    flow_rate_gpm_leak = max(flow_rate_gpm_leak, 0)
    flow_rate_lps_leak = flow_rate_gpm_leak * 0.06309 / 10  # Divide by 10

    # Calculate velocity
    velocity_leak = (flow_rate_lps_leak) / ((math.pi * pipe_diameter_meters**2) / 4)

    # Calculate acceleration for leak data
    acceleration_leak = (
        (velocity_leak - previous_velocity_leak) / time_interval
        if previous_velocity_leak is not None
        else 0
    )

    previous_velocity_leak = velocity_leak

    base_pressure_leak = 60.0
    pressure_variation_leak = random.uniform(-1.0, 1.0)
    pressure_leak = (base_pressure_leak + pressure_variation_leak) * (
        flow_rate_gpm_leak / base_flow_rate_gpm_leak
    )  # Adjust pressure based on flow rate
    pressure_leak = max(pressure_leak, 0)
    time_interval = 1
    amount_passed_liter_leak = flow_rate_lps_leak * time_interval
    total_water_passed_leak += amount_passed_liter_leak
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"Timestamp: {timestamp}, Flow Rate (Leak): {flow_rate_lps_leak:.4f} L/s, Pressure (Leak): {pressure_leak:.2f} PSI, Amount Passed (Leak): {amount_passed_liter_leak:.4f} liters, Total Water Passed (Leak): {total_water_passed_leak:.4f} liters, Velocity (Leak): {velocity_leak:.4f} m/s, Acceleration (Leak): {acceleration_leak:.4f} m/s^2"
    )

    insert_query_leak = "INSERT INTO randomdata_sensor_leak(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_data_leak = (
        timestamp,
        flow_rate_lps_leak,
        pressure_leak,
        amount_passed_liter_leak,
        total_water_passed_leak,
        velocity_leak,
        acceleration_leak,
    )
    cursor_leak.execute(insert_query_leak, insert_data_leak)
    db_connection.commit()

    base_flow_rate_gpm_no_leak = 12.0
    flow_rate_variation_no_leak = random.uniform(-0.1, 0.1)
    flow_rate_gpm_no_leak = base_flow_rate_gpm_no_leak + flow_rate_variation_no_leak
    flow_rate_gpm_no_leak = max(flow_rate_gpm_no_leak, 0)
    flow_rate_lps_no_leak = flow_rate_gpm_no_leak * 0.06309 / 10  # Divide by 10

    # Calculate velocity
    velocity_no_leak = (flow_rate_lps_no_leak) / (
        (math.pi * pipe_diameter_meters**2) / 4
    )

    # Calculate acceleration for no-leak data
    acceleration_no_leak = (
        (velocity_no_leak - previous_velocity_no_leak) / time_interval
        if previous_velocity_no_leak is not None
        else 0
    )

    previous_velocity_no_leak = velocity_no_leak

    base_pressure_no_leak = 60.0
    pressure_variation_no_leak = random.uniform(-1.0, 1.0)
    pressure_no_leak = (base_pressure_no_leak + pressure_variation_no_leak) * (
        flow_rate_gpm_no_leak / base_flow_rate_gpm_no_leak
    )  # Adjust pressure based on flow rate
    pressure_no_leak = max(pressure_no_leak, 0)
    amount_passed_liter_no_leak = flow_rate_lps_no_leak * time_interval
    total_water_passed_no_leak += amount_passed_liter_no_leak
    print(
        f"Timestamp: {timestamp}, Flow Rate (No Leak): {flow_rate_lps_no_leak:.4f} L/s, Pressure (No Leak): {pressure_no_leak:.2f} PSI, Amount Passed (No Leak): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (No Leak): {total_water_passed_no_leak:.4f} liters, Velocity (No Leak): {velocity_no_leak:.4f} m/s, Acceleration (No Leak): {acceleration_no_leak:.4f} m/s^2"
    )

    insert_query_no_leak = "INSERT INTO RandomData_Sensor_NoLeak(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_data_no_leak = (
        timestamp,
        flow_rate_lps_no_leak,
        pressure_no_leak,
        amount_passed_liter_no_leak,
        total_water_passed_no_leak,
        velocity_no_leak,
        acceleration_no_leak,
    )
    cursor_no_leak.execute(insert_query_no_leak, insert_data_no_leak)
    db_connection.commit()

    print(
        f"Timestamp: {timestamp}, Flow Rate (Sensor): {flow_rate_lps_no_leak:.4f} L/s, Pressure (Sensor): {pressure_no_leak:.2f} PSI, Amount Passed (Sensor): {amount_passed_liter_no_leak:.4f} liters, Total Water Passed (Sensor): {total_water_passed_no_leak +2:.4f} liters, Velocity (Sensor): {velocity_no_leak:.4f} m/s, Acceleration (Sensor): {acceleration_no_leak:.4f} m/s^2"
    )
    insert_query_sensor = "INSERT INTO RandomData_Sensor(timestamp, flow_rate, pressure, amount_passed, total_water_passed, velocity, acceleration) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_data_sensor = (
        timestamp,
        flow_rate_lps_no_leak,
        pressure_no_leak,
        amount_passed_liter_no_leak,
        total_water_passed_no_leak + 10,
        velocity_no_leak,
        acceleration_no_leak,
    )
    cursor_sensor.execute(insert_query_sensor, insert_data_sensor)
    db_connection.commit()

    # Fetch data from randomdata_sensor_leak
    cursor_leak.execute(
        "SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1"
    )
    data_leak = cursor_leak.fetchone()

    # Fetch data from randomdata_sensor_no_leak
    cursor_no_leak.execute(
        "SELECT timestamp, flow_rate, total_water_passed FROM RandomData_Sensor_NoLeak ORDER BY timestamp DESC LIMIT 1"
    )
    data_no_leak = cursor_no_leak.fetchone()

    # Fetch data from randomdata_sensor
    cursor_sensor.execute(
        "SELECT timestamp, flow_rate, total_water_passed FROM randomdata_sensor ORDER BY timestamp DESC LIMIT 1"
    )
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
        insert_data_nrw = (
            data_leak[0],
            nrw_diff,
            flowrate_diff,
            nrw_diff,
            flowrate_diff,
        )
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
        insert_data_zero_nrw = (
            data_no_leak[0],
            nrw_diff_zero,
            flowrate_diff_zero,
            nrw_diff_zero,
            flowrate_diff_zero,
        )
        cursor_zero_nrw.execute(insert_query_zero_nrw, insert_data_zero_nrw)
        db_connection.commit()

    # Fetch data from zero_non_revenue_water
    cursor_zero_nrw.execute(
        "SELECT timestamp, nrw, flowrate_diff FROM zero_non_revenue_water ORDER BY timestamp DESC LIMIT 1"
    )
    data_zero_nrw = cursor_zero_nrw.fetchone()

    # Fetch data from non_revenue_water
    cursor_leak.execute(
        "SELECT timestamp, nrw, flowrate_diff FROM non_revenue_water ORDER BY timestamp DESC LIMIT 1"
    )
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

    time.sleep(1)
