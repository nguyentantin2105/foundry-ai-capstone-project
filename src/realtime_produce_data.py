import random
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer
from mimesis import Person, Address
from mimesis.enums import Locale

# Faker generators
person = Person(Locale.EN)
address = Address(Locale.EN)

merchant_prefix = ["Super", "Global", "Fast", "Green", "Happy", "Digital", "Bright"]
merchant_suffix = ["Mart", "Store", "Shop", "Outlet", "Bazaar", "Market", "Online"]

# Kafka Producer
producer = KafkaProducer(
    # bootstrap_servers='localhost:29092',
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: x.encode('utf-8')
)

print("🚀 Real-time streaming to Kafka (no duplicate transactions)...")

try:
    seen_transaction_ids = set()  # Optional: if you want to double-check uniqueness

    while True:
        transaction_id = str(uuid.uuid4())
        while transaction_id in seen_transaction_ids:
            transaction_id = str(uuid.uuid4())  # Ensure uniqueness (extreme case)

        seen_transaction_ids.add(transaction_id)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        amount = round(random.uniform(5.0, 10000.0), 2)
        merchant = f"{random.choice(merchant_prefix)} {random.choice(merchant_suffix)}"
        category = random.choice(["Food", "Electronics", "Clothing", "Entertainment", "Travel", "Gaming"])
        customer_id = person.identifier()
        location = address.city()
        device_type = random.choice(["Mobile", "Desktop", "POS", "ATM"])

        # Build the raw message
        raw_line = f"{timestamp} {transaction_id} {merchant} {amount} {category} {customer_id} {location} {device_type}"

        # Send to Kafka
        producer.send("transaction_stream", value=raw_line)
        print(f"✅ Sent: {raw_line}")

        time.sleep(0.1)

except KeyboardInterrupt:
    print("🛑 Stopped by user.")
finally:
    producer.flush()
    producer.close()

