from kafka import KafkaConsumer, KafkaProducer
import json
import re
import string

# -- Configuration --
BOOTSTRAP_SERVERS = 'localhost:29092'
INPUT_TOPIC = 'transaction_stream'
OUTPUT_TOPIC = 'transaction_stream_processed'

# -- Parser Function --
def parse_log_line(line):
    # Updated regex to allow 1 or 2 decimal digits in amount
    pattern = re.compile(
        r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) '      # timestamp
        r'([\w-]{36}) '                                 # transaction_id
        r'(.+?) '                                       # merchant_name
        r'(\d+\.\d{1,2}) '                              # amount
        r'(\w+) '                                       # category
        r'([\d/-]+ [A-Za-z ]+) '                        # location
        r'(\w+)$'                                       # device_type
    )

    match = pattern.match(line)
    if not match:
        return None

    timestamp, transaction_id, merchant_name, amount, category, location, device_type = match.groups()

    return {
        "timestamp": timestamp,
        "transaction_id": transaction_id,
        "merchant_name": merchant_name.strip(),
        "amount": float(amount),
        "category": category,
        "location": location.strip(),
        "device_type": device_type
    }

# -- Kafka Consumer & Producer --
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: m.decode('utf-8', errors='replace')  # tolerate decoding errors
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"👂 Listening on topic: {INPUT_TOPIC}")

def clean_line(line):
    # Remove non-printable characters and leading/trailing spaces
    return ''.join(c for c in line.strip() if c in string.printable)

for message in consumer:
    raw_line = clean_line(message.value)

    parsed_data = parse_log_line(raw_line)

    if parsed_data:
        print(f"✅ Parsed: {parsed_data}")
        producer.send(OUTPUT_TOPIC, parsed_data)
    else:
        print(f"❌ Failed to parse: {repr(raw_line)}")  # show hidden characters
