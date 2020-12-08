#DB Params
DB_HOST = 'localhost'
DB_NAME = 'proyecto_db'
DB_USER = 'postgres'
DB_PASS = '123456'

import psycopg2
import psycopg2.extras
import random
import string

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST , port=5432)


def genText():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def poblate_by_clients(size):
    client_cache = []
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                id = random.randint(10000000, 99999999)
                while id in client_cache:
                    id = random.randint(10000000, 99999999)
                client_cache.append(id)
                curr.execute("INSERT INTO Grot.Empresa (ruc, direccion, razonSocial) VALUES("+str(id)+ ", ' " + genText()+ " ',"+ str(id) +" )")
                conn.commit()
                print(1)

    #End session                
    curr.close()
    conn.close()

def poblate_by_warehouse(size):
    warehouse_cache = []

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                
                dir = genText()
                while dir in warehouse_cache:
                    dir = genText()
                warehouse_cache.append(dir)
                curr.execute("INSERT INTO Grot.almacen (direccion , capacidad ) VALUES( ' "+str(dir)+ " ' , " + str(random.randint(1000, 10000)) +" )")
                conn.commit()

    #End session                
    curr.close()
    conn.close()



def poblate_by_products(size):
    products_cache = []

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                
                id = random.randint(1, size * 10)
                while id in products_cache:
                    id =  random.randint(1, size * 10)
                products_cache.append(id)
                curr.execute("INSERT INTO Grot.producto (id  , nombre ) VALUES( "+ str(id)+ "  ,  ' " + genText() +" ' )")
                conn.commit()

    #End session                
    curr.close()
    conn.close()

def get_keys(keys, table, rows = ""):
    row = []
    if(str(rows) != ""):
        rows = "Limit " +  str(rows)

    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
            curr.execute("SELECT " + str(keys)  +" FROM Grot." + str(table)  + " TABLESAMPLE BERNOULLI(1) " + rows)
            row = curr.fetchall ()
            conn.commit()
    #End session                
    curr.close()
    conn.close()

    return row

if __name__ == "__main__":
    row = get_keys("*" , "empresa")
    for x in row:
        print(x)
            