import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_RECORDS = 5000   # change this (e.g., 1000000 for 1 million rows)
OUTPUT_FILE = "employee_data_large.csv"

# Sample data
first_names = ["Amit", "Riya", "John", "Neha", "Rahul", "Pooja", "Arjun", "Kavita", "Manish", "Sara"]
last_names = ["Sharma", "Verma", "Singh", "Gupta", "Kumar", "Mehta", "Nair", "Joshi", "Patil", "Iyer"]

departments = ["IT", "HR", "Finance", "Sales", "Marketing"]
locations = ["Delhi", "Mumbai", "Bangalore", "Pune", "Hyderabad", "Chennai", "Noida", "Gurgaon"]
statuses = ["active", "inactive"]

# Function to generate random date
def random_date(start_year=2018, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

# Generate CSV
with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # Header
    writer.writerow([
        "id", "name", "department", "location",
        "salary", "joining_date", "status"
    ])

    # Data rows
    for i in range(1, NUM_RECORDS + 1):
        name = random.choice(first_names) + " " + random.choice(last_names)
        department = random.choice(departments)
        location = random.choice(locations)
        salary = random.randint(30000, 100000)
        joining_date = random_date()
        status = random.choice(statuses)

        writer.writerow([
            i, name, department, location,
            salary, joining_date, status
        ])

print(f"Generated {NUM_RECORDS} records in {OUTPUT_FILE}")
