import psycopg2
from tqdm import tqdm
from datetime import datetime
import re

SOCAT_FILE = 'SOCATv2025.tsv'
SOCAT_FILE_LENGTH = 41387663
DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_NAME = 'socat_kpi'

conn = psycopg2.connect(database = DB_NAME, 
                        user = DB_USER, 
                        host= DB_HOST,
                        password = DB_PASSWORD)

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS socat")
conn.commit()

cur.execute("""CREATE TABLE socat(
    platform_code text,
    expocode text,
    time timestamp,
    position geometry(Point, 4326),
    fco2 float,
    fco2_flag integer
    );""")
conn.commit()

f = open(SOCAT_FILE)

with tqdm(total=SOCAT_FILE_LENGTH) as progress:
    expocode_count = 0
    while expocode_count < 3:
        line = f.readline()
        if line.startswith('Expocode'):
            expocode_count += 1
        progress.update()
        
    line = f.readline()
    record_count = 0
    progress.update()
    while line != '':
        fields = line.split('\t')
        expocode = fields[0]
        if '-' in expocode:
            platform_code = re.search('(.*)\\d{8}-\\d$', expocode)[1]
        else:
            platform_code = re.search('(.*)\\d{8}$', expocode)[1]

        seconds = int(fields[9][0:2])
        if seconds > 59:
            seconds = 59
        timestamp = datetime(int(fields[4]), int(fields[5]), int(fields[6]), int(fields[7]), int(fields[8]), seconds)
        fco2 = float(fields[29])
        fco2_flag = int(fields[31])

        lon = float(fields[10])
        if lon > 180:
            lon = (360 - lon) * -1
        
        cur.execute(f"""INSERT INTO socat VALUES 
        ('{platform_code}', '{expocode}', '{timestamp}', ST_GeomFromText('POINT({lon} {float(fields[11])})', 4326), '{fco2}', '{fco2_flag}')""")

        record_count += 1
        if record_count % 10000 == 0:
            conn.commit()

        progress.update()
        line = f.readline()
        
conn.commit()

f.close()
cur.close()
conn.close()
