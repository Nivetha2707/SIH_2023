<?php
$servername = "localhost";
$username = "root";
$password = "";
$dbname = "sensor_data";

// Create connection
$conn = new mysqli($servername, $username, $password, $dbname);

// Check connection
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}

// Fetch data from non_revenue_water table
$sql = "SELECT timestamp, total_water_passed FROM randomdata_sensor_leak ORDER BY timestamp DESC LIMIT 1"; // Adjust the LIMIT as needed
$result = $conn->query($sql);

$data = array('timestamps' => array(), 'total_water_passed' => array());

if ($result->num_rows > 0) {
    while ($row = $result->fetch_assoc()) {
        $data['timestamps'][] = $row['timestamp'];
        $data['total_water_passed'][] = $row['total_water_passed'];
    }
}

echo json_encode($data);

$conn->close();
?>