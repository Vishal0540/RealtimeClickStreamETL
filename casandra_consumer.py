from kafka import KafkaConsumer

from dotenv import load_dotenv

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

from ssl import PROTOCOL_TLSv1_2, CERT_REQUIRED , CERT_NONE
import json 
import os



# Load environment variables from .env file
load_dotenv()


def connet_to_cassandra():
    """
    Connects to the Cassandra database.

    Reads the required credentials and configuration from a .env file in the same directory.

    Returns:
        A Cassandra session object on successful connection.

    Raises:
        Exception: If there is an error during the connection attempt.

    Environment Variables (stored in .env file):
        - CASSANDRA_USERNAME: The username for Cassandra authentication.
        - CASSANDRA_PASSWORD: The password for Cassandra authentication.
        - CASSANDRA_HOST: The hostname of the Cassandra cluster.
        - CASSANDRA_PORT: (Optional) The port number for Cassandra. Defaults to 10350 if not specified.
    """

    ssl_opts = {
    'ssl_version': PROTOCOL_TLSv1_2,
    'cert_reqs': CERT_NONE 
    }

    # Read credentials from environment variables
    cassandra_username = os.getenv('CASSANDRA_USERNAME')
    cassandra_password = os.getenv('CASSANDRA_PASSWORD')
    cassandra_host = os.getenv('CASSANDRA_HOST')
    cassandra_port = int(os.getenv('CASSANDRA_PORT', 10350))


    
    try:
        
        auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
        
        cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider, ssl_options=ssl_opts)
        
        session = cluster.connect("clickstream")
        
        # Return the session on success

        print("Connected!")

        return session

    except Exception as e:
        print(f"Error connecting to Cassandra: {str(e)}")
        raise

    


def insert_to_cansandra(cassandra_session , row_to_insert):

    insert_query = f"""
    INSERT INTO clickstream_events (row_key, user_id, timestamp, url, country, city, browser, os, device)
    VALUES ('{row_to_insert['row_key']}', '{row_to_insert['user_id']}', '{row_to_insert['timestamp']}', '{row_to_insert['url']}', '{row_to_insert['country']}', '{row_to_insert['city']}', '{row_to_insert['browser']}', '{row_to_insert['os']}', '{row_to_insert['device']}');
    """

    cassandra_session.execute(insert_query)


if __name__ == "__main__":


    with open("config.json" , "r") as fl:
        config_data = json.load(fl)

    cassandra_session = connet_to_cassandra()


    # Conneting Consumer to read from last record where it left , incase first time or restart it will read from 
    # earilest record
    consumer = KafkaConsumer(config_data['topic'], bootstrap_servers=config_data['bootstrap_servers'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group')

    

    print("Ready to Consume Data.....")

    while True:

        try:
            for message in consumer:
            
                clickstream_data = json.loads(message.value)

                print(clickstream_data)


                print("=="*10)

                try:
                    insert_to_cansandra(cassandra_session , clickstream_data)
                except:
                    pass

        except KeyboardInterrupt:

            print("Consumer Stopped!")

            break