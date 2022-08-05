# broken

import threading

import duckdb

con = duckdb.connect(database=':memory:')
cursor = con.cursor()
# con.execute('CREATE TABLE test (a INTEGER, b INTEGER)')
# con.execute('INSERT INTO test VALUES (1, 2)')
# con.execute('INSERT INTO test VALUES (3, 4)')
# con.execute('INSERT INTO test VALUES (5, 6)')

def execute_query(query):
    try:
        print(query)
        print(cursor.execute(query).fetchall())
    except Exception as e:
        print(e)

try:
    print('Starting threads...')
    threading.Thread(target=execute_query, args=('SELECT * FROM test',)).start()
    threading.Thread(target=execute_query, args=('CREATE TABLE x (a INTEGER, b INTEGER)',)).start()
except Exception as e:
    print(e)
