from mysqlconnectivity_project import *
from credential import  *
import pandas as pd
import boto3
from io import StringIO

# To create Data Frame

df_city=sakila_table('city','df_city')
df_actor=sakila_table('actor','df_actor')
df_address=sakila_table('address','df_address')
df_category=sakila_table('category','df_category')
df_country=sakila_table('country','df_country')
#df_customer=sakila_table('customer','df_customer')
df_film=sakila_table('film','df_film')
df_film_actor=sakila_table('film_actor','df_film_actor')
df_film_text=sakila_table('film_text','df_film_text')
df_inventory=sakila_table('inventory','df_inventory')
df_language=sakila_table('language','df_language')
# df_payment=sakila_table('payment','df_payment')
# df_rental=sakila_table('rental','df_rental')
# df_staff=sakila_table('staff','df_staff')
df_store=sakila_table('store','df_store')

##To create Pandas data Frame

df_city_pd=df_city.toPandas()
df_actor_pd=df_actor.toPandas()
df_address_pd=df_address.toPandas()
df_category_pd=df_category.toPandas()
df_country_pd=df_country.toPandas()
#df_customer_pd=df_customer.toPandas()
df_film_pd=df_film.toPandas()
df_film_actor_pd=df_film_actor.toPandas()
df_film_text_pd=df_film_text.toPandas()
df_inventory_pd=df_inventory.toPandas()
df_language_pd=df_language.toPandas()
# df_payment_pd=df_payment.toPandas()
# df_rental_pd=df_rental.toPandas()
# df_staff_pd=df_staff.toPandas()
df_store_pd=df_store.toPandas()

#To Define function for to load all files on S3 location

def aws_s3_connect(pd_name,file_name):
    bucket='final-project-du'
    csv_buffer=StringIO()
    pd_name.to_csv(csv_buffer)
    s3_resource=boto3.resource('s3')
    s3_resource.Object(bucket,file_name).put(Body=csv_buffer.getvalue())


# To call the function for to load data to s3 location

aws_s3_connect(df_city_pd,'df_city_pd')
aws_s3_connect(df_actor_pd,'df_actor_pd')
aws_s3_connect(df_address_pd,'df_address_pd')
aws_s3_connect(df_category_pd,'df_category_pd')
aws_s3_connect(df_country_pd,'df_country_pd')
aws_s3_connect(df_film_pd,'df_film_pd')
aws_s3_connect(df_film_actor_pd,'df_film_actor_pd')
aws_s3_connect(df_film_text_pd,'df_film_text_pd')
aws_s3_connect(df_inventory_pd,'df_inventory_pd')
aws_s3_connect(df_language_pd,'df_language_pd')
aws_s3_connect(df_store_pd,'df_store_pd')
# aws_s3_connect(df_payment_pd,'df_payment_pd')
# aws_s3_connect(df_rental_pd,'df_rental_pd')
# aws_s3_connect(df_staff_pd,'df_staff_pd')








