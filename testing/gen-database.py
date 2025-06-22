#!/usr/bin/env python3

import sqlite3

from faker import Faker

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

# Create the user table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL
    )
""")

product_list = [
    "hat",
    "cap",
    "shirt",
    "sweater",
    "sweatshirt",
    "shorts",
    "jeans",
    "sneakers",
    "boots",
    "coat",
    "accessories",
]

fake = Faker()
for _ in range(10000):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()
    age = fake.random_int(min=1, max=100)

    cursor.execute(
        """
        INSERT INTO users (first_name, last_name, email, phone_number, address, city, state, zipcode, age)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (first_name, last_name, email, phone_number, address, city, state, zipcode, age),
    )

for product in product_list:
    price = fake.random_int(min=1, max=100)
    cursor.execute(
        """
        INSERT INTO products (name, price)
        VALUES (?, ?)
    """,
        (product, price),
    )


conn.commit()
conn.close()
