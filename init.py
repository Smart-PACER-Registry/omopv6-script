# Reading an excel file using Python

import pandas as pd
import numpy as np
from psycopg2 import connect, sql, DatabaseError
import psycopg2.extras as extras
from configparser import ConfigParser
from datetime import datetime

name_of_table = "concept"

column_name_map = {'Element OMOP Concept ID' : 'concept_id',
                   'Element OMOP Concept Name' : 'concept_name',
                   'Vocabulary' : 'vocabulary',
                   'Element OMOP Concept Code' : 'concept_code'}

parser = ConfigParser()

def config(filename='config.ini'):
    parser.read(filename)

def getConfig(section, inverse=False):

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            if inverse:
                db[param[1]] = param[0]
            else:
                db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the file'.format(section))

    return db

def connectdb():

    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = getConfig('postgresql')

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = connect(**params)
		
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
    except (Exception, DatabaseError) as error:
        print(error)
    return conn

def getNextConcept(cur):

    cur.execute("SELECT MAX(concept_id) FROM %s;" % name_of_table)
    max_val = cur.fetchone()[0]
    return max_val + 1

def insertConcept(conceptid) :
    # insert into database
    sql = "INSERT INTO %s (" % name_of_table

def getById(cur, id):
    
    cur.execute("SELECT * FROM %s WHERE concept_id = %s;" % (name_of_table, id))
    concept_table = cur.fetchone()
    return concept_table

def updateConcept(conn, row):

    sql = "UPDATE concept SET concept_name = %s, concept_code = %s WHERE concept_id = %s" % (row['concept_name'],
    row['concept_code'], row['concept_id'])
    cur.execute(sql)
    conn.commit()

def insertConcept(conn, row):

    cur = conn.cursor()

     # Get next available concept id
    conceptid = getNextConcept(cur)

    start_index = int(getConfig('database')['start_index'])

    if conceptid < start_index:
        conceptid = start_index

    print('Next concept id: %s' % str(conceptid))
    
    # Retreive default table update
    concept = getConfig('default')
    concept["concept_id"] = str(conceptid)
    concept["concept_name"] = str(conceptid)
    
    # execute_batch(cur, insert_str, default_params)
    columns = ", ".join(list(concept.keys()))
    values = list(concept.values())

    sql = "INSERT INTO concept (%s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (str(columns), 
    values[0], str(values[1]), str(values[2]), str(values[3]), str(values[4]), str(values[5]), 
    str(values[6]), str(values[7]), str(values[8]), str(values[9]))
    cur.execute(sql)
    conn.commit()

    return concept

def needChange(row, concept):
    hasChange = False

config()

# Create db connection
conn = connectdb()
cur = conn.cursor()

#get excel config
e = getConfig('excel')

# Read excel 
wb = pd.read_excel(e['name'], sheet_name=e['sheet'])

# Update columns base on column mapping
cwb = wb.rename(columns = getConfig('mapping', True))

inversecolumn = getConfig('mapping', False)

for index, row in cwb.iterrows():

    #Create mapping frame 
    print(row['concept_id'], row['concept_name'], row['vocabulary'], row['concept_code'])

    modified = False
    concept = {}
    if np.isnan(row['concept_id']):
        concept = insertConcept(conn, row)
        modified=True
    else:     
        concept = getById(cur, row['concept_id'])
        modified=needChange(row, concept)
        if modified:
            updateConcept(conn, row)

    if modified:
        wb.loc[index, inversecolumn['concept_id']] = str(concept['concept_id'])
        wb.loc[index, inversecolumn['concept_name']] = str(concept['concept_name'])
        wb.loc[index, inversecolumn['concept_code']] = str(concept['concept_code'])

print(wb)
wb.to_excel(e['name'], index=False)

if conn is not None:
    conn.close()
    print('Database connection closed.')
