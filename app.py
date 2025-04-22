import pandas as pd
import mysql.connector
import csv
import os
def load_csv(file_path):
    df=pd.read_csv(file_path)
    return df

def process_csv(df):
  df.dropna(inplace=True) #remove rows that are empty or null
  df.drop_duplicates(inplace=True) #remove duplicate rows
  df['date'] = pd.to_datetime(df['date'])
  return df
#connect to mysql database
def connect_to_mysql():
    mydb=mysql.connector.connect(
    user='root',
    password='20f01@05F6',
    host='localhost',
    database='mydatabase'
    )
    return mydb

def create_table(df,mydb):
    
    cursor=mydb.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS prescriptions( id INT AUTO_INCREMENT PRIMARY KEY,
    patient_name VARCHAR(100),
    age INT,
    gender VARCHAR(10),
    doctor_name VARCHAR(100),
    diagnosis VARCHAR(255),
    medicines TEXT,
    dosage TEXT,
    date DATE,
    location VARCHAR(20))""")
    cursor.execute("TRUNCATE TABLE prescriptions;")
    #insert data into mysql
    for _,row in df.iterrows():
        cursor.execute(""" insert into prescriptions(patient_name,age,gender,doctor_name,diagnosis,medicine,dosage,date,location) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",tuple(row))
    mydb.commit()
def insights(mydb):
    queries=[{"key":"1","title":"Prescribing_Trends","query":"SELECT medicine, COUNT(*) AS count FROM prescriptions GROUP BY medicine ORDER BY count DESC LIMIT 10;"},
             {"key":"2","title":"Drug_Dosage_Utilization","query": "SELECT age,gender, medicine, COUNT(*) AS count FROM prescriptions GROUP BY age,gender, medicine order by age,gender,medicine;"},
             {"key":"3","title":"Anomalies","query":"SELECT doctor_name, COUNT(*) AS count FROM prescriptions GROUP BY doctor_name ORDER BY count DESC;"},
             {"key":"4","title":"Geographic_Analysis","query":"SELECT location, medicine, COUNT(*) AS count FROM prescriptions GROUP BY location, medicine;"},
             {"key":"5","title":"Demographic_Analysis","query":"SELECT age_group, diagnosis, COUNT(*) AS count FROM (SELECT *,CASE WHEN age BETWEEN 0 AND 18 THEN '0-18' WHEN age BETWEEN 19 AND 35 THEN '19-35' WHEN age BETWEEN 36 AND 50 THEN '36-50' WHEN age BETWEEN 51 AND 65 THEN '51-65' ELSE '65+' END AS age_group FROM prescriptions) AS grouped GROUP BY age_group, diagnosis order by age_group,diagnosis;"}
             ]
            
    cursor=mydb.cursor()
    repeat = True
    while repeat :
      for option in queries:
        print(f"{option['key']}.{option['title']}")
      print('Press 0 to Exit\n')
      user_input=input("Enter Your Choice : ")
      for option in queries:
        if option['key'] == user_input:
            cursor.execute(option['query'])
            print(f"{option['title']}")
            columns=[desc[0] for desc in  cursor.description]
            rows = cursor.fetchall()
            filename = f"reports/{option['title']}.csv"
            with open(filename,'w') as f:
                    writer=csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)
            print(columns,end='\n')
            for row in rows:
                    print(row,end='\n')
            print("\n")
            break
      if user_input == '0':
          repeat = False
        
        

              
def main():
    file_path=input('Enter file name: ')
    if not os.path.isfile(file_path):
        print(f'No File Named {file_path}')
        return
    df=load_csv(file_path)
    df=process_csv(df)
    mydb=connect_to_mysql()
    create_table(df,mydb)
    insights(mydb)
    mydb.close()

if __name__ == '__main__':
    main()
