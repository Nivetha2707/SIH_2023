import mysql.connector
from twilio.rest import Client
from datetime import datetime, timedelta
import time

# MySQL database configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "sensor_data",
}

# Twilio API configuration
twilio_account_sid = "ACe6823381435fdf2fa8137"
twilio_auth_token = "a85bc36af54b773182b"
twilio_phone_number = "+12055286264"
recipient_phone_number = "9370465"


# Function to check the database and send SMS if condition is met
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

                # Check if the condition is met (NRW value > 1 within the last minute)
                if nrw_value > 1 and time_difference < timedelta(minutes=1):
                    # Send SMS using Twilio
                    client = Client(twilio_account_sid, twilio_auth_token)
                    message = client.messages.create(
                        body=f"High NRW alert: NRW value is {nrw_value}, Flowrate Diff: {flowrate_diff}, at {timestamp}",
                        from_=twilio_phone_number,
                        to=recipient_phone_number,
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


# Run the function
check_and_send_sms()
