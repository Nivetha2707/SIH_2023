import mysql.connector
from mysql.connector import Error
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time

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
    password = "duij ugpf qilu fjqn"

    # Email content
    subject = "Alert: NRW Value Exceeded 10"
    body = f"The latest NRW value is {value}, which is greater than 10."

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

if __name__ == "__main__":
    # Connect to MySQL
    db_connection = connect_to_mysql()

    if db_connection:
        try:
            while True:
                # Fetch the latest NRW value
                latest_nrw = fetch_latest_nrw(db_connection)

                if latest_nrw is not None:
                    print(f"Latest NRW value: {latest_nrw}")

                    # Check if NRW value is greater than 10
                    if latest_nrw > 10:
                        # Send email and end the program
                        send_email(latest_nrw)
                        break

                # Wait for 1 second before checking again
                time.sleep(1)

        except KeyboardInterrupt:
            print("Program ended by user.")
        finally:
            # Close the database connection
            db_connection.close()
