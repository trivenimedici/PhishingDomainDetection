from asyncore import read
import enum
from os import listdir
import os
import shutil
from cassandra.cluster import Cluster
import cassandra
from cassandra.auth import PlainTextAuthProvider
import csv
from app_logger.logger import APP_LOGGER
class Data_Insertion_ToDB:
    def __init__(self):
        self.logger=APP_LOGGER()
        self.filePathforDB='Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
    

    def dataBaseConnection(self):
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
            cloud_config={'secure_connect_bundle':r'secure-connect-test.zip'}
            auth_provider=PlainTextAuthProvider('mRGABWqgHJdFTdijYddyBNcc','75z65hndaa_.GOxqQ,SmEuk8kovbK6MN+gUIX04t.tH.vQ7cH8_R8k+sUEaWXyL6vpkIPHUyT2wxviJ44gQJkNPogqeMFlhbE5kcGwCpMXr.6ts.pPCMkxRowvaAFBN,','AstraCS:mRGABWqgHJdFTdijYddyBNcc:126c3951ca64f1e43a73a8ef3dcf6d74f04f3d268db5230785094d5ceb1f9483')
            cluster=Cluster(cloud=cloud_config,auth_provider=auth_provider)
            session=cluster.connect()
            file=open("Log_Files_Collection/Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file,"Opened  database successfully" )
            file.close()
        except ConnectionError as e:
            file = open("Log_Files_Collection/Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return session

    def createTableDb(self,keyvalue,column_names):
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
            conn = self.dataBaseConnection()
            conn.execute("SELECT count(table_name) FROM system_schema.tables WHERE keyspace_name='"+keyvalue+"'")
            if conn.fetchone()[0] ==1:
                file = open("Log_Files_Collection/Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                conn.close()
                file = open("Log_Files_Collection/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed  database successfully" )
                file.close()
            else:
                for key in column_names.keys():
                    type = column_names[key]
                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                
                file = open("Log_Files_Collection/Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                conn.close()    
                file = open("Log_Files_Collection/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed database successfully" )
                file.close()
        except Exception as e:
            file = open("Log_Files_Collection/Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Log_Files_Collection/Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed  database successfully" )
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
        log_file = open("Log_Files_Collection/Training_Logs/DbInsertLog.txt", 'a+')
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
        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Log_Files_Collection/Training_Logs/ExportToCsv.txt", 'a+')
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

        