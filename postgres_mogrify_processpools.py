import psycopg2
from concurrent.futures import ProcessPoolExecutor
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
        rows = [tuple(row) for row in reader]
    return rows


def insert_rows(rows: list, conn: object) -> None:
    cur = conn.cursor()
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode('utf8') for row in rows)
    cur.execute("INSERT INTO trip_data VALUES " + args_str)
    conn.commit()


def chunker(lst, n):
    chunks = [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]
    return chunks


def file_insert(file: str):
    conn = connect_postgres()
    print(f"working on file {file}")
    rows = read_file(file)
    chunks = chunker(rows, 50000)
    for chunk in chunks:
        insert_rows(chunk, conn)
    print(f"finished with file {file}")


if __name__ == '__main__':
    t1 = datetime.now()
    files = pull_data_files()
    with ProcessPoolExecutor(max_workers=5) as poolparty:
        poolparty.map(file_insert, files)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")
