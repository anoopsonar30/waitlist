#!/usr/bin/python
import psycopg2
import random 
import string # imported for testing purposes
from config import config

def test_connection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	    # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def drop_tables():
    """ Delete the registrations and waitlist table from the database """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command for dropping/deleting tables
        dropTableSQL = "DROP TABLE registrations, waitlist"

        # execute the delete tables command
        cur.execute(dropTableSQL)

        # commit changes to database
        conn.commit()
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_tables():
    """ Create the registrations and waitlist table in the PostgreSQL database.
    Uncomment the first 2 commands if you get a citext error """
    commands = (
        # """
        # CREATE EXTENSION citext
        # """,
        # """
        # CREATE DOMAIN email_id AS citext
        #     CHECK ( value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$' );
        # """,
        """
        CREATE TABLE registrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email email_id UNIQUE
        )
        """,
        """ 
        CREATE TABLE waitlist (
            waitid SERIAL PRIMARY KEY,
            id INTEGER NOT NULL,
            referrals INTEGER NOT NULL,
            FOREIGN KEY (id)
                REFERENCES registrations (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )

    conn = None
    try:
        # read the connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        # create table one by one
        for command in commands:
            cur.execute(command)
        print("Tables created.")

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def register_user(name, email):
    """ Register a new user to the tables in the database """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
        # SQL command for adding user to registrations table
        insertUserRegSQL = """
            INSERT INTO registrations(name, email)
            VALUES(%s, %s) RETURNING id;
            """
        
        # execute command for inserting user to the registrations table
        cur.execute(insertUserRegSQL, (name, email))
        
        # fetch the returning id from the execute command
        id = cur.fetchone()[0]
        # Print the returned ID
        print("ID : ", id)
        
        # commit changes to the database
        conn.commit()

        # SQL command for adding the new user to the waitlist
        insertUserWaitSQL = """
            INSERT INTO waitlist(id, referrals)
            VALUES(%s, %s) RETURNING waitid;
            """
        
        # Add user to the waitlist table with it's id and 
        # number of referrals (0 by default)
        cur.execute(insertUserWaitSQL, (id, 0))

        # fetch the returning waitid
        waitid = cur.fetchone()[0]
        print("WaitID : ", waitid)

        # commit changes to database
        conn.commit()       

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            # print('Database connection closed.') 

def get_user_id(name, email):
    """ Get the User ID corresponding to a given name and email """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor() 

        # SQL command for finding the ID for a given user
        # with name and email
        sql = """
        SELECT id FROM registrations WHERE name = %s AND email = %s
        """
        # execute command and get ID for the given user
        cur.execute(sql, (name, email))
        id = cur.fetchone()
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return id

def get_rank(id):
    """ Get the rank for a user with a particular id based on the number of referrals 
    and how early they signed up """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command for finding the rank of the user with given id
        getRow = """
        WITH temp AS (SELECT id, ROW_NUMBER() OVER (ORDER BY referrals DESC) AS row
        FROM waitlist) 
        SELECT row FROM temp WHERE id = %s 
        """

        # Execute command to find rank
        cur.execute(getRow, (id, ))
        rank = cur.fetchone()
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return rank

def get_ranks_for_all():
    """ Returns a tuple of IDs and their ranks for all users """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command for returning list of ranks sorted by referrals
        getRow = """
        SELECT id, ROW_NUMBER() OVER (ORDER BY referrals DESC)
        FROM waitlist
        """

        # Execute command and fetch ranks for all users
        cur.execute(getRow)
        ranks = cur.fetchall()

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return ranks

def get_position_from_rank(rank, multiplier):
    """ Compute the position using the rank and the multiplier
     and offset by some random value """

    position = (multiplier * rank) + random.randint(0, multiplier)
    
    return position
    # If you want to start the ranking from 2000 
    # return (position + 2000)

def get_positions_for_all(ranks, multiplier, seed):
    """ Compute positions for all ranks """
    random.seed(seed)
    positions = [(ranks[i][0], get_position_from_rank(ranks[i][1], multiplier)) 
    for i in range (len(ranks))]
    
    return positions

def increment_referral_count(id):
    """ Increment the number of referrals for a given user """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command to read the number of referrals the user has 
        readSql = """
        SELECT referrals
        FROM waitlist
        WHERE id = %s
        """
        # Execute command and extract # of referrals
        cur.execute(readSql, (id,))
        curReferrals = cur.fetchone()[0]

        # Print current # of referrals
        print("Old # of referrals : ", curReferrals)

        # Increment the number of referrals by 1
        curReferrals = curReferrals + 1

        # SQL command to update the number of referrals for a user
        updateSql = """
        UPDATE waitlist
        SET referrals = %s
        WHERE id = %s
        """

        # Execute UPDATE command and commit changes to database
        cur.execute(updateSql, (curReferrals, id))
        conn.commit()  

        # Print the new number of referrals for the user
        print("New # of Referrals : ", curReferrals)
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def remove_from_waitlist(id):
    """ Remove a user from the waitlist table """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command to delete user from the waitlist
        sql = """ DELETE FROM waitlist WHERE id = %s """

        # execute command to remove user with given ID and commit changes to database
        cur.execute(sql, (id,))
        conn.commit()

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def remove_from_registrations(id):
    """ Remove a user from the registrations table, 
    which also removes it from the waitlist table """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        # SQL command to delete a user with given ID
        sql = """ DELETE FROM registrations WHERE id = %s """

        # execute command to delete the user and commit changes to database
        cur.execute(sql, (id,))
        conn.commit()
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def add_random_users(numRandUsers):
    """ Adds numRandUsers new random users to the database. 
    This function exists solely for testing purposes."""
    # we will generate random permutations of all lower case letters
    letters = string.ascii_lowercase   
    
    # generate new random names, emails and register them to the database
    for _ in range(numRandUsers):
        name = ( ''.join(random.choice(letters) for i in range(8)))
        email = ( ''.join(random.choice(letters) for i in range(8))) + '@gmail.com'
        register_user(name, email)