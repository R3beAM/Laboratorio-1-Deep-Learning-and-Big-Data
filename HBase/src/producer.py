from kafka import KafkaProducer
import json
import time
import ast

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    try: 
        data = ast.literal_eval(input("Enter data to send (or 'exit' to quit): "))
        if data == 'exit':
            break
        elif isinstance(data, dict):
            producer.send('user-topic', value=data)
            print(f"Produced: {data}")
        else:
            print("Invalid input. Please enter a dictionary.")
    except Exception as e:
        print("Invalid input. Please enter a dictionary.")


producer.flush()
producer.close()

# Examples

# {'name': 'John', 'age': 25}
# {'name': 'Alice', 'age': 30, 'city': 'New York', 'hobby': 'painting'}
# {'name': 'Bob', 'age': 22, 'city': 'Los Angeles'}
# {'name': 'Charlie', 'age': 28, 'city': 'Chicago', 'hobby': 'reading', 'occupation': 'engineer'}
