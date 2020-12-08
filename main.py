#DB Params
DB_HOST = 'localhost'
DB_NAME = 'proyecto_db'
DB_USER = 'postgres'
DB_PASS = 'niarfe+456'

import psycopg2
import psycopg2.extras
import random
import string

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST , port=5433)


def genText():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def generate_comprobante(size):
    comprobante_cache = []
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                numero = random.randint(1000, 9999)
                while numero in comprobante_cache:
                    numero = random.randint(1000, 9999)
                comprobante_cache.append(numero)
                start_date = datetime.date(2020, 1, 1)
                end_date = datetime.date(2020, 2, 1)
                time_between_dates = end_date - start_date
                days_between_dates = time_between_dates.days
                random_number_of_days = random.randrange(days_between_dates)
                random_date = start_date + datetime.timedelta(days=random_number_of_days)   
                random_cost = random.randint(100, 1000000)
                random_type = (random.randint(1,2) == 1)
                curr.execute("INSERT INTO Grot.comprobante (numero , fecha , precio, tipo) VALUES("+ str(numero) + ", " + str(random_date) + ", "+ str(random_cost) + ", "+ str(random_type) +")")
                conn.commit()
    #End session                
    curr.close()
    conn.close()    

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
            