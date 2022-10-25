
import mysql.connector as conn
from timeit import default_timer as timer
from datetime import timedelta
from mysql.connector import errorcode

DB_NAME = 'employees'

TABLES = {}
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "  `emp_no` int(11) NOT NULL AUTO_INCREMENT,"
    "  `birth_date` date NOT NULL,"
    "  `first_name` varchar(14) NOT NULL,"
    "  `last_name` varchar(16) NOT NULL,"
    "  `gender` enum('M','F') NOT NULL,"
    "  `hire_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB;")

TABLES['departments'] = (
    "CREATE TABLE `departments` ("
    "  `dept_no` char(4) NOT NULL,"
    "  `dept_name` varchar(40) NOT NULL,"
    "  PRIMARY KEY (`dept_no`), UNIQUE KEY `dept_name` (`dept_name`)"
    ") ENGINE=InnoDB;")

TABLES['salaries'] = (
    "CREATE TABLE `salaries` ("
    "  `emp_no` int(11) NOT NULL,"
    "  `salary` int(11) NOT NULL,"
    "  `from_date` date NOT NULL,"
    "  `to_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`,`from_date`), KEY `emp_no` (`emp_no`),"
    "  CONSTRAINT `salaries_ibfk_1` FOREIGN KEY (`emp_no`) "
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB;")

TABLES['dept_emp'] = (
    "CREATE TABLE `dept_emp` ("
    "  `emp_no` int(11) NOT NULL,"
    "  `dept_no` char(4) NOT NULL,"
    "  `from_date` date NOT NULL,"
    "  `to_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`,`dept_no`), KEY `emp_no` (`emp_no`),"
    "  KEY `dept_no` (`dept_no`),"
    "  CONSTRAINT `dept_emp_ibfk_1` FOREIGN KEY (`emp_no`) "
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE,"
    "  CONSTRAINT `dept_emp_ibfk_2` FOREIGN KEY (`dept_no`) "
    "     REFERENCES `departments` (`dept_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB;")

TABLES['dept_manager'] = (
    "  CREATE TABLE `dept_manager` ("
    "  `emp_no` int(11) NOT NULL,"
    "  `dept_no` char(4) NOT NULL,"
    "  `from_date` date NOT NULL,"
    "  `to_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`,`dept_no`),"
    "  KEY `emp_no` (`emp_no`),"
    "  KEY `dept_no` (`dept_no`),"
    "  CONSTRAINT `dept_manager_ibfk_1` FOREIGN KEY (`emp_no`) "
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE,"
    "  CONSTRAINT `dept_manager_ibfk_2` FOREIGN KEY (`dept_no`) "
    "     REFERENCES `departments` (`dept_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB;")

TABLES['titles'] = (
    "CREATE TABLE `titles` ("
    "  `emp_no` int(11) NOT NULL,"
    "  `title` varchar(50) NOT NULL,"
    "  `from_date` date NOT NULL,"
    "  `to_date` date DEFAULT NULL,"
    "  PRIMARY KEY (`emp_no`,`title`,`from_date`), KEY `emp_no` (`emp_no`),"
    "  CONSTRAINT `titles_ibfk_1` FOREIGN KEY (`emp_no`)"
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB;")

TABLES['customer_info'] = (
    "CREATE TABLE `customer_info` ("
    "  `contractId` int(11) NOT NULL,"
    "  `AccountNumber` int(11) NOT NULL,"
    "  `CreatedAt` varchar(250) NOT NULL,"
    "  `Amount` int(11) NOT NULL,"
    "  `Count` int(11) NOT NULL,"
    "  `Duration` int(11) NOT NULL"
    ") ENGINE=InnoDB;")

TABLES['analysis_table'] = (
    "CREATE TABLE `analysis_table` ("
    "  `action_type` varchar(250) NOT NULL,"
    "  `start_time` varchar(250) NOT NULL,"
    "  `end_time` varchar(250) NOT NULL,"
    "  `duration` double(10,2) NOT NULL,"
    "  `others` varchar(250) NOT NULL"
    ") ENGINE=InnoDB;")



def createTables(cursor,dbConnect):

    try:
        
        cursor.execute("USE `"+str(DB_NAME)+"`;")

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: "+str(table_name), end='\n')
                cursor.execute(table_description)
            except conn.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            # else:
            #     print("OK")
        cursor.execute("SHOW TABLES;")
        cursor.close()
        dbConnect.close()

    except conn.Error as err:
        print("Database {} does not exists."+str(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database {} created successfully."+str(DB_NAME))
            dbConnect.database = DB_NAME
        else:
            print(err)

def createDB():
    
    dbConnect = conn.connect(
        host="localhost",
        user="root",
        password="XXXXX"
    )
    # Cursor to the database
    start = timer()
    cursor = dbConnect.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS "+str(DB_NAME))
    except conn.Error as err:
        print("Failed creating database: {}"+str(err))
    
    createTables(cursor,dbConnect)
    end = timer()
    print("Elapsed Time "+str(timedelta(seconds=end-start)))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    createDB()
