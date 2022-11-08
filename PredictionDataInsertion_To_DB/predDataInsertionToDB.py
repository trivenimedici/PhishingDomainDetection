from asyncore import read
import enum
from os import listdir
import os
import shutil
import sqlite3
import csv
from app_logger.logger import APP_LOGGER
class Data_Insertion_ToDB:
    def __init__(self):
        self.logger=APP_LOGGER()
        self.filePathforDB='Prediction_Database/'
        self.badFilePath = "Prediction_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_files_validated/Good_Raw"
    

    def dataBaseConnection(self,databaseName):
        """
            Method Name: dataBaseConnection
            Description: This method is to create database connection
            Output:  Connection to the DB   
            On Failure: Raise ConnectionError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            conn=sqlite3.connect(self.filePathforDB+databaseName+'.db')
            file=open("Log_Files_Collection/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file,"Opened %s database successfully" % databaseName)
            file.close()
        except ConnectionError as e:
            file = open("Log_Files_Collection/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names):
        """
            Method Name: createTableDb
            Description: This method is to create table in the database created
            Output:  None
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            c=conn.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] ==1:
                file = open("Log_Files_Collection/Prediction_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                conn.close()
                file = open("Log_Files_Collection/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()
            else:
                for key in column_names.keys():
                    type = column_names[key]
                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                
                file = open("Log_Files_Collection/Prediction_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                conn.close()    
                file = open("Log_Files_Collection/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()
        except Exception as e:
            file = open("Log_Files_Collection/Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Log_Files_Collection/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e
    def insertIntoTableGoodData(self,Database):
        """
            Method Name: insertIntoTableGoodData
            Description: This method is to insert records into the table for the db
            Output:  None
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        conn=self.dataBaseConnection(Database)
        goodFilesPath=self.goodFilePath
        badFilesPath=self.badFilePath
        files=[f for f in listdir(goodFilesPath)]
        log_file = open("Log_Files_Collection/Prediction_Logs/DbInsertLog.txt", 'a+')
        for f in files:
            try:
                with open(goodFilesPath+'/'+f, "r") as f:
                    next(f)
                    reader=csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file," %s: File loaded successfully!!" % f)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilesPath+'/' + f, badFilesPath)
                self.logger.log(log_file, "File Moved Successfully %s" % f)
                log_file.close()
                conn.close()  
        conn.close()
        log_file.close()  
    def selectingDatafromtableintocsv(self,Database):
        """
            Method Name: selectingDatafromtableintocsv
            Description: This method is insert all the table from db tables to csv file
            Output:  None
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Log_Files_Collection/Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn=self.dataBaseConnection(Database)
            selectQuerry='SELECT * FROM Good_Raw_Data'
            cursor=conn.cursor()
            cursor.execute(selectQuerry)
            results=cursor.fetchall()
            headers=[i[0] for i in cursor.description]
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)
            csvfile=csv.writer(open(self.fileFromDb+self.fileName,'w',newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            csvfile.writerow(headers)
            csvfile.writerows(results)
            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()
        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()

        