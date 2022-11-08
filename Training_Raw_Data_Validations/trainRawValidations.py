from app_logger.logger import APP_LOGGER
import os
import pandas as pd
import json
import shutil
import re
import datetime
class RawValidations:
    def __init__(self,path):
        self.filepath=path
        self.schema_path="training_schema.json"
        self.logger=APP_LOGGER()
    

    def valuesFromDataSchema(self):
        """
            Method Name: valuesFromDataSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output:  column names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()
            no_of_columns = dic['NoOfColumns']
            col_names=dic['columnnames']
            file=open('Log_Files_Collection/Training_Logs/schemaValidationsLogs.txt','a+')
            self.logger.log(file,f"the number of coumns are {no_of_columns} and the column names are {col_names}")
            file.close()
        except ValueError:
            file=open('Log_Files_Collection/Training_Logs/schemaValidationsLogs.txt','a+')
            self.logger.log(file,"ValueError:Value not found inside the schema.json")
            raise ValueError
        except KeyError:
            file=open('Log_Files_Collection/Training_Logs/schemaValidationsLogs.txt','+a')
            self.logger.log(file,"KeyValueError:Key value error incorrect key passed")
            raise KeyError
        except Exception as e:
            file=open('Log_Files_Collection/Training_Logs/schemaValidationsLogs.txt','+a')
            self.logger.log(file,str(e))
            raise e
        return no_of_columns,col_names
                
    def regexFileCreation(self):
        """
            Method Name: regexFileCreation
            Description: This method is to create regex based on the file name given in schema file
                Output:  manual_regex_pattern
            On Failure: None
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        manual_regex_pattern="dataset_[a-z0-9*]+.csv"
        return manual_regex_pattern
    
    def validationFileNameRaw(self,regex):
        """
            Method Name: validationFileNameRaw
            Description: This method is to validate the file name for the given input datasets
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.deleteExistingGoodDataTrainingFolder()
        self.deleteExistingBadTrainingDataFiles()
        #self.deleteExistingResultPredictionDataFiles()
        self.createDirForGoodBadData()
        onlyfiles=[f for f in os.listdir(self.filepath)]
        try:
            file=open('Log_Files_Collection/Training_Logs/nameValidationLog.txt','a+')
            for filename in onlyfiles:
                if(re.match(regex,filename)):
                    shutil.copy("Batch_Training_DataSet/" + filename, "Training_Raw_files_validated/Good_Raw")
                    self.logger.log(file,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                else:
                    shutil.copy("Batch_Training_DataSet/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(file,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            file.close()
        except Exception as e:
            f = open("Log_Files_Collection/Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e
    
    def validateColumnLength(self,NumberofColumns):
        """
            Method Name: validateColumnLength
            Description: This method is to validate number of columns for the input dataset file
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            f = open("Log_Files_Collection/Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation started!!")
            for file in os.listdir('Training_Raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/'+file)
                if csv.shape[1]==NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Log_Files_Collection/Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Log_Files_Collection/Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validatingMissingValuesinWholeColumn(self):
        """
            Method Name: validatingMissingValuesinWholeColumn
            Description: This method is to value the missing values in the column for the given input dataset file
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            f = open("Log_Files_Collection/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")
            for file in os.listdir('Training_Raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/'+file)
                count=0
                for columns in csv:
                    if(len(csv[columns])-csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,"Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    #csv.rename(columns={"Unnamed: 0": "phishing"}, inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("Log_Files_Collection/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Log_Files_Collection/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    
    def createDirForGoodBadData(self):
        """
            Method Name: createDirForGoodBadData
            Description: This method creates directories to store the Good Data and Bad Data after validating the training data.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path=os.path.join('Training_Raw_files_validated/','Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path=os.path.join('Training_Raw_files_validated/','Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            # path=os.path.join('Result_Dataset_File/','input_data/')
            # if not os.path.isdir(path):
            #     os.makedirs(path)
        except OSError as e:
            file=open('ErrorLogs.txt','a+')
            self.logger.log(file,"Error while creating directory %s:"% e)
            file.close()
            raise OSError
    
    def deleteExistingGoodDataTrainingFolder(self):
        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data after loading the data in the table. Once the good files are loaded in the DB,deleting the directory ensures space optimization.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
                file=open('Log_Files_Collection/Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,"Good Raw directory deleted successfully!!")
                file.close()
        except Exception as e:
            file=open("Log_Files_Collection/Training_Logs/ErrorLogs.txt", 'a+')
            self.logger.log(file,"Error while deleting directory : %s" %e)
            file.close()
            raise e

    def deleteExistingBadTrainingDataFiles(self):
        """
            Method Name: deleteExistingBadTrainingDataFiles
            Description: This method deletes the directory made  to store the bad Data after loading the data in the table. Once the bad files are loaded in the DB,deleting the directory ensures space optimization.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+'Bad_Raw/')
                file=open('Log_Files_Collection/Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,"Bad Raw directory deleted successfully!!")
                file.close()
        except OSError as e:
            file=open("Log_Files_Collection/Training_Logs/ErrorLogs.txt", 'a+')
            self.logger.log(file,"Error while deleting directory : %s" %e)
            file.close()
            raise e
    def moveBadFilesToArchiveBad(self):
        """
            Method Name: moveBadFilesToArchiveBad
            Description: This method is to move all the bad data files to archieve folder
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.now = datetime.now()
        self.date = self.now.date()
        self.time = self.now.strftime("%H%M%S")
        try:

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(self.date)+"_"+str(self.time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Log_Files_Collection/Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Log_Files_Collection/Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e


            


