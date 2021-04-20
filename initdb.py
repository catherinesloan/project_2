import os
import csv
from sqlalchemy import create_engine, Table, Column, Float, Integer, String, MetaData

meta = MetaData()
connection = os.environ.get('DATABASE_URL', '') or "sqlite:///db.sqlite"

# electoral_division table 
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


# election_results table 
if not engine.has_table("election_results"):
    print("Creating Table")

    new_table = Table(
        'election_results', meta,
        Column('division_id', Integer, primary_key=True),
        Column('enrolment', Integer),
        Column('demographic', String),
        Column('previous_party', String),
        Column('previous_seat_status', String),
        Column('successful_party', String),
        Column('seat_status', String)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/02-output_election_results/02-election_results.csv', newline='') as input_file:
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


# election_vote_types table
if not engine.has_table("election_vote_types"):
    print("Creating Table")

    new_table = Table(
        'election_vote_types', meta,
        Column('division_id', Integer, primary_key=True),
        Column('ordinary_votes', Integer),
        Column('absent_votes', Integer),
        Column('provisional_votes', Integer),
        Column('prepoll_votes', Integer),
        Column('postal_votes', Integer),
        Column('total_votes', Integer)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/03-output_election_vote_types/election_vote_types.csv', newline='') as input_file:
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


# election_turnout table
if not engine.has_table("election_turnout"):
    print("Creating Table")

    new_table = Table(
        'election_turnout', meta,
        Column('division_id', Integer, primary_key=True),
        Column('total_enrolled', Integer),
        Column('total_votes', Integer),
        Column('turnout_percent', Float)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/04-ouput_election_turnout/election_turnout.csv', newline='') as input_file:
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


# marriage_postal_results table
if not engine.has_table("marriage_postal_results"):
    print("Creating Table")

    new_table = Table(
        'marriage_postal_results', meta,
        Column('division_id', Integer, primary_key=True),
        Column('yes_count', Integer),
        Column('no_count', Integer),
        Column('total_responses', Integer),
        Column('response_unclear', Integer),
        Column('non_responding', Integer)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/05-output_marriage_postal_results/marriage_postal_results.csv', newline='') as input_file:
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


# marriage_postal_turnout table
if not engine.has_table("marriage_postal_turnout"):
    print("Creating Table")

    new_table = Table(
        'marriage_postal_turnout', meta,
        Column('division_id', Integer, primary_key=True),
        Column('total_eligible', Integer),
        Column('total_participants', Integer),
        Column('turnout_percent', Float)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/06-output_marriage_postal_turnout/marriage_postal_turnout.csv', newline='') as input_file:
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


# marriage_postal_participants_by_age
if not engine.has_table("marriage_postal_participants_by_age"):
    print("Creating Table")

    new_table = Table(
        'marriage_postal_participants_by_age', meta,
        Column('division_id', Integer, primary_key=True),
        Column('ages_18_34', Integer),
        Column('ages_35_49', Integer),
        Column('ages_50_64', Integer),
        Column('ages_65_79', Integer),
        Column('ages_80_plus', Integer)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/07-output_marriage_postal_participants_by_age/marriage_postal_participants_by_age.csv', newline='') as input_file:
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


# cultural_diversity table
if not engine.has_table("cultural_diversity"):
    print("Creating Table")

    new_table = Table(
        'cultural_diversity', meta,
        Column('division_id', Integer, primary_key=True),
        Column('aboriginal_torres_strait_percent', Float),
        Column('born_overseas_percent', Float),
        Column('recent_migrants_percent', Float),
        Column('different_language_percent', Float)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/09-output_cultural_diversity/09-cultural_diversity.csv', newline='') as input_file:
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


# education table
if not engine.has_table("education"):
    print("Creating Table")

    new_table = Table(
        'education', meta,
        Column('division_id', Integer, primary_key=True),
        Column('year_12_completion_percent', Float),
        Column('higher_education_completion_percent', Float)
    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/10-output_education/10-education.csv', newline='') as input_file:
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


# labor_liberal_votes
if not engine.has_table("labor_liberal_votes"):
    print("Creating Table")

    new_table = Table(
        'labor_liberal_votes', meta,
        Column('division_id', Integer, primary_key=True),
        Column('liberal_votes', Integer),
        Column('liberal_percent', Float),
        Column('labor_votes', Integer),
        Column('labor_percent', Float)

    )
    
    meta.create_all(engine)
    
    print("Table created")

    """
    Let's read in the csv data and put into a list to read into
    our newly created table
    """
    seed_data = list()

    with open('data/11-output_labor_liberal_vote_counts/11-labor_liberal_votes.csv', newline='') as input_file:
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


# print statement to check
print("initdb complete")