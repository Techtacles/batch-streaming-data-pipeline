from faker import Faker
from kafka import KafkaProducer
import json
from dotenv import dotenv_values
import random

env_var=dotenv_values('.env')
fake=Faker()


producer=KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:json.dumps(x).encode('utf-8'))

def create_stream(topic_name):
    while True:
        first_name=fake.first_name()
        last_name=fake.last_name()
        address=fake.address()
        email=fake.email()
        credit_no=fake.credit_card_number()
        company=fake.company()
        quantity=fake.random_digit_not_null_or_empty()
        price=random.randint(50000,1000000)
        producer_data={'first_name':first_name,'last_name':last_name,'address':address,'email':email,
                    'credit_no':credit_no,'company':company,'quantity':quantity,'price':price}
        producer.send(topic_name,producer_data)        
        print(f'Sending messages to {topic_name}')
    return producer_send

create_stream(env_var['topic_name'])