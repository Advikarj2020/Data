import mysql.connector
import pandas as pd
import boto3
from io import StringIO


mydb = mysql.connector.connect(
    host='database-1.cqswgamhpoj2.ap-south-1.rds.amazonaws.com',
    user='admin',
    password='12345678',
    port='3306',
    database= 'sakila'
)

mycursor1 = mydb.cursor()

mycursor1.execute('SELECT * FROM actor')

users = mycursor1.fetchall()
#print(users)

df1 = pd.read_sql('SELECT * FROM actor where actor_id between 1 and 10', mydb)
print(df1)

def upload_s3(df1,i):
    s3 = boto3.client("s3",aws_access_key_id="AKIAUJ5FAXI5F7X2L2N7",aws_secret_access_key="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")
    csv_buf = StringIO()
    df1.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket="advika-2020", Body=csv_buf.getvalue(), Key='Rahul/'+i)

    upload_s3(df1, "rrr.csv")
