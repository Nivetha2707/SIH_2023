import random
import time

def generate_flowrate_data():
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

        # Adjust the sleep duration based on how often you want to generate data
        time.sleep(1)  # Sleep for 1 second

# Uncomment the line below to start generating realistic flow rate data
generate_flowrate_data()
