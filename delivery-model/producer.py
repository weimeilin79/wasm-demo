import csv
import time
from kafka import KafkaProducer


# Define Kafka producer configuration
kafka_topic = 'raw-data'  # Replace with your Kafka topic name

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=['localhost:19092','localhost:29092','localhost:39092'])

# Define the CSV file path
csv_file_path = 'deliverytime.txt'  # Replace with your CSV file path

# Batch size for sending messages to Kafka
batch_size = 1000

# Read the CSV file and send rows to Kafka
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # This skips the title row
    batch = []
    for row in csv_reader:
        # Send each row as a message to the Kafka topic
        message = ','.join(row).encode('utf-8')
        producer.send(kafka_topic, value=message)

        # Add the row to the batch
        batch.append(message)

        # Check if the batch size is reached, then send the batch and clear it
        if len(batch) >= batch_size:
            producer.flush()
            print(f'Sent {len(batch)} rows to Redpanda.')
            batch = []

            # Sleep for 3 seconds
            time.sleep(1)

    # Send any remaining rows in the last batch
    if batch:
        producer.flush()
        print(f'Sent {len(batch)} rows to Redpanda.')

# Close the Kafka producer
producer.close()