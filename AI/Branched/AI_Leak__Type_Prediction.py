from tensorflow.keras.models import load_model
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd
from collections import Counter
import mysql.connector
from sklearn.preprocessing import LabelEncoder
import warnings

# Ignore specific TensorFlow deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="tensorflow")


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
print(new_data)
# Load the pre-trained model
loaded_model = load_model('Leak_Prediction.keras')  # Replace 'my_model.keras' with the actual file name


# Preprocess the new data (scaling)
scaler = StandardScaler()
new_data_scaled = scaler.fit_transform(new_data)

# Make predictions
predictions = loaded_model.predict(new_data_scaled)

# Get the predicted class with the highest probability for each sample
predicted_labels = predictions.argmax(axis=1)


#creating counter object
count= Counter(predicted_labels)
#maximume number of occurences
max_count = max(count.most_common(), key=lambda x: x[1])

if(max_count[0]==0):
    print("Predicted Leak Type: Circumfrential Crack")
elif(max_count[0]==1):
    print("Predicted Leak Type: Gasket Leak")
elif(max_count[0]==2):
    print("Predicted Leak Type: Longitudinal Crack")
elif(max_count[0]==3):
    print("Predicted Leak Type: Non Leak")
else:
    print("Predicted Leak Type: Orifice Leak")
