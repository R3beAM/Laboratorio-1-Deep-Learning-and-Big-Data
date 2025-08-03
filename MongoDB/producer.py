from pymongo import MongoClient
import time
import random

def main():
    client = MongoClient("mongodb://admin:admin123@localhost:27017/")
    db = client["company"]
    employees = db["employees"]

    names = ["Laura Díaz", "Carlos Pérez", "Ana Gómez", "Juan Ríos", "Sofía Mena"]
    departments = ["Ventas", "TI", "Marketing", "RRHH", "Finanzas"]

    employee_id = 1
    while True:
        doc = {
            "employee_id": employee_id,
            "name": random.choice(names),
            "department": random.choice(departments),
            "salary": random.randint(30000, 80000),
            "processed": False
        }
        employees.insert_one(doc)
        print(f"Inserted: {doc}")
        employee_id += 1
        time.sleep(3)

if __name__ == "__main__":
    main()

