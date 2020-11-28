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

client_cache = []

def genText():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def poblate_by_clients(size):
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                id = random.randint(10000000, 99999999)
                while id in client_cache:
                    id = random.randint(412135, 432658)
                client_cache.append(id)
                curr.execute("INSERT INTO Grot.Empresa (ruc, direccion, razonSocial) VALUES("+str(id)+ ", ' " + genText()+ " ',"+ str(id) +" )")
                conn.commit()

    #End session                
    curr.close()
    conn.close()

def poblate_by_warehouse(size):
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                id = random.randint(10000000, 99999999)
                while id in client_cache:
                    id = random.randint(412135, 432658)
                client_cache.append(id)
                curr.execute("INSERT INTO Grot.almacen (direccion , descripcion , capacidad ) VALUES("+str(id)+ ", ' " + genText()+ " ',"+ str(id) +" )")
                conn.commit()

    #End session                
    curr.close()
    conn.close()


if __name__ == "__main__":
    poblate_by_clients(100)