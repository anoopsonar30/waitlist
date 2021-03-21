#!/usr/bin/python
# Author - Anoop Sonar
# Scalable Waitlist Backend API using Python + Postgres
# Comments - 
# 1)Postgres uses an LRU cache so I'm not sure if caching the top 2000 positions 
# will necessarily improve performance (depends on why you need it), but we can adjust 
# the size of the shared_buffer for postgres to be equal to 2000 rows of data.
# 2) If by "buffer" you meant the first 2000 positions are "reserved" that can be
# implemented easily by adding 2000 to the output of the rank_to_position() function.
# 3) Note that in this (and in any practical implementation), the position of a user
# on the waitlist can go higher after registration in some cases if too many other people
# give out successful referrals. This is a theoretical limitation unless you decide to 
# give them the same position as someone else.
# 4) It took ~80 seconds to add 10,000 users (numRandUsers = 10000) on my computer so the server
# should be able to handle 10,000 users over the course of a day.

from utils import *
import time

if __name__ == '__main__':

    # Test Connection and print Postgres database version
    test_connection()
    
    # Delete the registration and waitlist tables
    drop_tables()
    
    # Create the registration and waitlist tables.
    # Uncomment the first 2 commands in create_tables() if you get a citext error.
    # This is the initial setup that is executed just once per database
    create_tables()

    # Add N random users to the database for testing purposes
    numRandUsers = 100
    tic = time.time()
    add_random_users(numRandUsers)
    toc = time.time()

    print("Time to add ", numRandUsers, " : ", (toc - tic))

    # Add a new user to the registration table and the waitlist table
    name = "Anoop"
    email = "asonar@princeton.edu"
    register_user(name, email)

    # Get the ID corresponding to a name and email
    id = get_user_id(name, email)[0]
    print("ID : ", id)

    # Increment the referral count of a particular id
    increment_referral_count(id)

    # Print the rank for all ID's on the waitlist
    ranks = get_ranks_for_all()
    print("Ranks : ", ranks)

    # Print the positions for all ID's on the waitlist
    seed = 4
    # Fix the seed for consistent positioning
    random.seed(seed)
    multiplier = 5
    positions = get_positions_for_all(ranks, multiplier)
    print("Positions : ", positions)

    # Access the rank of a particular id on the waitlist.
    # Initially the rank was (numRandUsers + 1) but 
    # after adding the referral it becomes 1 
    rank = get_rank(id)[0]
    print("Rank for id ", id, " : ", rank)

    # Print the position corresponding to a particular rank 
    position = get_position_from_rank(rank, multiplier)
    print("Position : ", position)

    # # Remove row corresponding to id from waitlist
    remove_from_waitlist("11")

    # Remove row corresponding to id from registrations (also removes from waitlist)
    remove_from_registrations("22")