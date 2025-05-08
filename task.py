import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import json
import os
import re
import urllib
from datetime import datetime



# Replace with your actual server and database name
server = 'DESKTOP-9EG77SQ\SQLEXPRESS'  # or just 'localhost'
database = 'Project'
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Load column map JSON
with open("column_map.json") as f:
    column_map = json.load(f)

# Function to load and decode a CSV based on its mapping
def load_and_decode(entry):
    file_path = entry["file"]
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    # Load CSV
    df = pd.read_csv(file_path)

    # Decode column names using mapping
    columns = {
        col: entry["columns"].get(col.split("-")[-1], col)
        for col in df.columns
    }
    df.rename(columns=columns, inplace=True)
    return df

# Loop through each table entry and decode
tables = {}
for table_id, entry in column_map.items():
    if table_id in {"BLCK", "FRD", "VIP"}:
        continue  # skip derived tables for now
    print(f"Loading {entry['table']} from {entry['file']}")
    df = load_and_decode(entry)
    if df is not None:
        tables[entry["table"]] = df

# Save decoded versions (optional)
for table_name, df in tables.items():
    df.to_csv(f"{table_name}.csv", index=False)
    print(f"Decoded table saved: {table_name}.csv")

# Users
users = tables["users"]
users["created_at"] = pd.to_datetime(users["created_at"], errors="coerce")
users["last_active_at"] = pd.to_datetime(users["last_active_at"], errors="coerce")
users["is_vip"] = users["is_vip"].astype(bool)
users["total_balance"] = pd.to_numeric(users["total_balance"], errors="coerce")

# Cards
cards = tables["cards"]
cards["created_at"] = pd.to_datetime(cards["created_at"], errors="coerce")
cards["balance"] = pd.to_numeric(cards["balance"], errors="coerce")
cards["limit_amount"] = pd.to_numeric(cards["limit_amount"], errors="coerce")

# Transactions
transactions = tables["transactions"]
transactions["created_at"] = pd.to_datetime(transactions["created_at"], errors="coerce")
transactions["amount"] = pd.to_numeric(transactions["amount"], errors="coerce")

# Clean phone numbers
users["phone_number"] = users["phone_number"].apply(
    lambda x: x if isinstance(x, str) and re.match(r"^\+?\d{10,15}$", x) else None
)

# Clean emails
users["email"] = users["email"].apply(
    lambda x: x if isinstance(x, str) and re.match(r"[^@]+@[^@]+\.[^@]+", x) else None
)

# Fill or flag missing values
users["phone_number"].fillna("MISSING", inplace=True)
users["email"].fillna("MISSING", inplace=True)

cards["exceeds_limit"] = cards["balance"] > cards["limit_amount"]
transactions["flagged_large_txn"] = transactions["amount"] > 10000
print("Cards exceeding limit:")
print(cards[cards["exceeds_limit"]])

print("Flagged transactions:")
print(transactions[transactions["flagged_large_txn"]])

# Create 'users' table
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name NVARCHAR(100),
        phone_number NVARCHAR(15),
        email NVARCHAR(100),
        created_at DATETIME,
        last_active_at DATETIME,
        is_vip BIT,
        total_balance DECIMAL(18, 2)
    )
""")
conn.commit()

# Create 'cards' table with foreign key to 'users'
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cards' AND xtype='U')
    CREATE TABLE cards (
        id INT PRIMARY KEY,
        user_id INT,
        card_number NVARCHAR(16),
        balance DECIMAL(18, 2),
        created_at DATETIME,
        card_type NVARCHAR(50),
        limit_amount DECIMAL(18, 2),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
""")
conn.commit()


def log_ingestion_metadata(
    source_file, total_rows, processed_rows, errors=0, notes=""
):
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-9EG77SQ\SQLEXPRESS;"  # change as needed
        "DATABASE=Project;"     # change as needed
        "Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO retrieveinfo (
            source_file, retrieved_at, total_rows, processed_rows, errors, notes
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        source_file,
        datetime.now(),
        total_rows,
        processed_rows,
        errors,
        notes
    ))
    conn.commit()
    cursor.close()
    conn.close()







