import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
db_user = 'postgres'
db_password = '1244'  # 
db_host = 'localhost'  # or your database host
db_port = '5432'  # Default PostgreSQL port
db_name = 'cleaned_data_db'

# Create a database connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load the cleaned CSV file into a DataFrame
file_path = r'C:\Users\Yibabe\Desktop\10academykifiyaAIMweek-7\data\cleaned_message_data.csv'
df = pd.read_csv(file_path)

# Push the DataFrame to the PostgreSQL table
df.to_sql('scraped_data', engine, if_exists='replace', index=False)

print("Data has been successfully pushed to the database.")

