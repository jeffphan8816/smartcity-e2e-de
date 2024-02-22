import os
import random
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import simplejson as json

LONDON_COORDINATES = {'latitude': 51.509865, 'longitude': -0.118092}
BIRMINGHAM_COORDINATES = {'latitude': 52.4862, 'longitude': -1.8904}

# Calculate movement increment
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_topic')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_topic')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_topic')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_topic')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_topic')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

random.seed(10)


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movements():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add random noise to the coordinates
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movements()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(20, 40),
        'direction': 'North-East',
        'make': 'Toyota',
        'model': 'Corolla',
        'year': 2019,
        'fuelType': 'hybrid'
    }


def generate_gps_data(device_id, timestamp):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(20, 40),
        'direction': 'North-East',
        'vehicleType': 'private'
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString',
        'location': location
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 30),
        'weatherCondition': random.choice(['sunny', 'rainy', 'cloudy', 'snowy']),
        'precipitation': random.uniform(0, 100),
        'humidity': random.uniform(0, 100),
        'windSpeed': random.uniform(0, 100),
        'airQualityIndex': random.uniform(0, 500),
    }


def generate_emergency_data(device_id, timestamp, incident_location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'incidentId': uuid.uuid4(),
        'location': incident_location,
        'incidentType': random.choice(['accident', 'fire', 'theft', 'medical', 'None']),
        'severity': random.choice(['low', 'medium', 'high']),
        'status': random.choice(['reported', 'in-progress', 'resolved']),
        'description': 'Some description of the incident'
    }


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report)
    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], 'camera_1',
                                                           vehicle_data['location'])
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_data = generate_emergency_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        # 'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
        # 'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'vehicle_1')
    except KeyboardInterrupt:
        print('Keyboard interrupt detected. Exiting...')
    except Exception as e:
        print(f'An error occurred: {e}')
