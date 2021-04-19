import os
import csv
from sqlalchemy import create_engine, Table, Column, Float, Integer, String, MetaData

meta = MetaData()

connection = os.environ.get('DATABASE_URL', '') or "sqlite:///db.sqlite"

print("connection to databse")
database_uri = os.environ.get("DATABASE_URL").replace("://","ql://",1)
engine = create_engine(database_uri)

if not engine.has_table("electoral_division"):
    print("Creating Table")

    new_table = Table(
        'electoral_division', meta,
        Column('division_id', Integer, primary_key=True),
        Column('electoral_division', String),
        Column('state', String)
    )

    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/01-output_electoral_division/electoral_division.csv', newline='') as input_file:
        reader = csv.DictReader(input_file)       #csv.reader is used to read a file
        for row in reader:
            seed_data.append(row)
    
    """
    With our newly created table let's insert the row we've read in
    and with that we're done
    """
    with engine.connect() as conn:
        conn.execute(new_table.insert(), seed_data)

    print("Seed Data Imported")
else:
    print("Table already exists")

print("initdb complete")