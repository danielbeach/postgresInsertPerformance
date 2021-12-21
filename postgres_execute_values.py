import psycopg2
from psycopg2 import extras
from psycopg2 import sql
import csv
from glob import glob
from datetime import datetime


def connect_postgres(host: str = 'localhost', user: str = 'postgres', pwd: str = 'postgres', port: int = 5432):
    try:
        conn = psycopg2.connect(f'postgresql://{user}:{pwd}@{host}:{port}')
        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def pull_data_files(loc: str = 'data/*csv') -> list:
    files = glob(loc)
    return files


def read_file(file: str) -> list:
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        rows = [row for row in reader]
    return rows


def insert_rows(rows: list, conn: object) -> None:
    cur = conn.cursor()
    inputs = [[row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]
                        , row[10], row[11], row[12]] for row in rows]
    query = "INSERT INTO trip_data VALUES %s"
    extras.execute_values(cur, query, inputs)
    conn.commit()


def chunker(lst, n):
    chunks = [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]
    return chunks


if __name__ == '__main__':
    t1 = datetime.now()
    conn = connect_postgres()
    files = pull_data_files()
    for file in files:
        print(f"working on file {file}")
        rows = read_file(file)
        chunks = chunker(rows, 50000)
        for chunk in chunks:
            insert_rows(chunk, conn)
        print(f"finished with file {file}")
    conn.close()
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")
