import os
from database_connect.databases.mongodb import MongoIO as mongo

client_url="mongodb+srv://Abhinesh:12345@phishing.o91yw.mongodb.net/?retryWrites=true&w=majority&appName=Phishing"
database_name ="Phishing"

def upload_files_to_mongodb(
    mongo_client_con_string,
    database_name,
    datasets_dir_name):
  
  for file in os.listdir(datasets_dir_name):
    if file.endswith('.csv'):
      file_name = file.split('.')[0]

      mongo_connection = mongo(
          client_url = mongo_client_con_string,
          database_name= database_name,
          collection_name= file_name
      )

      file_path = os.path.join(datasets_dir_name, file)
      print(file_path)
      mongo_connection.bulk_insert(file_path)
      print(f"{file_name} is uploaded to mongodb")


upload_files_to_mongodb(
    mongo_client_con_string= client_url,
    database_name = database_name,
    datasets_dir_name= r"D:\Phishing\upload_data_to_db")