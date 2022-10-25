import mysql.connector as mysql
from mysql.connector import Error
from timeit import default_timer as timer
from datetime import timedelta
import csv



import pymysql


DB_NAME = 'employees'
conn = mysql.connect(
        host="localhost",
        user="root",
        password="XXXXX",
        database ="employees"
    )
if conn.is_connected():
    cursor = conn.cursor()

    csv_data = csv.reader(open('customer_info.csv'))
    header = next(csv_data)
    count = 0
    print('Importing the CSV Files')
    start = timer()

    try:
        for row in csv_data:
            print(row)
            cursor.execute(
                "INSERT INTO `customer_info` (`contractId`,`AccountNumber`,`CreatedAt`,`Amount`,`Count`,`Duration`) VALUES (%s, %s, %s, %s, %s, %s)", row)
            count += 1
    except Error as err:
        print("Error while connecting to MySQL!", err)

    end = timer()

    action = 'insert'
    start_time = start
    end_time = end
    duration = end-start
    others = 'time analysis for '+str(count)+' rows'
    try:
        sql = "INSERT INTO `"+str(DB_NAME)+"`.analysis_table (`action_type`,`start_time`,`end_time`,`duration`,`others`) VALUES ('"+str(action)+"','"+str(start_time)+"','"+str(end_time)+"','"+str(duration)+"','"+str(others)+"');"
        
        cursor.execute(sql)
    except Error as err:
        print("Error while connecting to MySQL", err)
    
    conn.commit()
    cursor.close()
    
    print('Imported Successfully')
    print("Elapsed Time "+str(timedelta(seconds=duration)))
