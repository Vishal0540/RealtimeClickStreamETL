import time
import random
import json
from datetime import datetime
import uuid

from faker import Faker
from kafka import KafkaProducer

def generate_uuid_from_name_and_timestamp(name, timestamp):
    # Concatenate the name and timestamp (as string) with a delimiter
    combined_string = f"{name}_{str(timestamp)}"
    
    # Create a UUID version 5 using the namespace UUID and the combined string
    namespace_uuid = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    generated_uuid = uuid.uuid5(namespace_uuid, combined_string)
    
    return generated_uuid

def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return int(unix_time(dt) * 1000.0)


def main():
    #Reading Configuration data
    with open("config.json" , "r") as fl:
        config_data = json.load(fl)



    producer = KafkaProducer(bootstrap_servers=config_data['bootstrap_servers'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    # Initialize Faker to generate random data
    fake = Faker()

    # Sample URLs
    urls = [
        "https://www.example.com/",
        "https://www.example.com/home",
        "https://www.example.com/about",
        "https://www.example.com/contact",
        "https://www.example.com/products",
        "https://www.example.com/services",
        "https://www.example.com/blog",
        "https://www.example.com/faq",
        "https://www.example.com/pricing",
        "https://www.example.com/login",
    ]

    # Sample users
    users = [fake.user_name() for _ in range(20)]\


    # Dictionary to store country and browser for each user
    user_info = {}

    # Generate and store country and browser for each user
    for user in users:
        user_info[user] = {
            "country": fake.country(),
            "browser": fake.user_agent().split('/')[0],
            "city":fake.city()
        }

    
    #Simulate continuous data generation 
    
    try:
        while True:
            
            time.sleep(random.uniform(0.5, 2.0))  # Sleep for random time between 0.5 and 2 seconds
            
            # Generate random data for each click event
            
            user_id = random.choice(users)
            timestamp = unix_time_millis(datetime.now())
            url = random.choice(urls)
            country = user_info[user_id]["country"]
            city = user_info[user_id]["city"]
            browser = user_info[user_id]["browser"]
            os = fake.user_agent().split(' ')[-1].split('(')[0]
            device = fake.user_agent().split('(')[-1].split(')')[0]

            #Identying each record uniuly from
            row_key =  generate_uuid_from_name_and_timestamp(user_id , timestamp)

            
            print(f"Row Key: {row_key}")
            print(f"Click Data: User ID: {user_id}, Timestamp: {timestamp}, URL: {url}")
            print(f"Geo Data: Country: {country}, City: {city}")
            print(f"User Agent Data: Browser: {browser}, OS: {os}, Device: {device}")
            print("\n")



            click_stream_data = {
                "row_key":str(row_key),
                "user_id":user_id,
                "timestamp":timestamp,
                "url" : url,
                "country" : country,
                "city" : city, 
                "browser" : browser,
                "os" : os,
                "device" : device
            }
    

            producer.send(config_data['topic'],click_stream_data)

    except KeyboardInterrupt:

        print("Data Producing Stoped")

        producer.flush()

if __name__ == "__main__":

    main()
