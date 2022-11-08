from Training_Raw_Data_Validations.trainRawValidations import RawValidations
from app_logger.logger import APP_LOGGER
from Training_DataTransformations.trainDataTransformations import Data_Transformations
from Training_DataInsertion_To_DB.trainDataInsertionToDB import Data_Insertion_ToDB
class train_validation:
    def __init__(self,path):
        self.raw_data=RawValidations(path)
        self.fileObject=open("Log_Files_Collection/Training_Logs/Training_Main_Log.txt","a+")
        self.log=APP_LOGGER()
        self.dbOperations=Data_Insertion_ToDB
        self.data_transform=Data_Transformations()

    def train_validation(self):
        try:
            self.log.log(self.fileObject,'Start of Validation on dataset files!!')
            no_of_columns,col_names=self.raw_data.valuesFromDataSchema()
            self.log.log(self.fileObject,f'the total columns to be added are {no_of_columns}')
            self.log.log(self.fileObject,f'the column names to be added are {col_names} and type is {type(col_names)}')
            regex=self.raw_data.regexFileCreation()
            self.raw_data.validationFileNameRaw(regex)
            self.raw_data.validateColumnLength(no_of_columns)
            self.raw_data.validatingMissingValuesinWholeColumn()
            self.log.log(self.fileObject, "Raw Data Validation Complete!!")
            self.log.log(self.fileObject, "Starting Data Transforamtion!!")
            self.data_transform.replaceMissingWithNull()
            self.log.log(self.fileObject, "DataTransformation Completed!!!")
            self.log.log(self.fileObject,"Creating Training_Database and tables on the basis of given schema!!!")
            # self.dbOperations.createTableDb('trainingdata',col_names)
            # self.log.log(self.fileObject, "Table creation Completed!!")
            # self.log.log(self.fileObject, "Insertion of Data into Table started!!!!")
            # self.dbOperations.insertIntoTableGoodData('Training')
            # self.log.log(self.fileObject, "Insertion in Table completed!!!")
            # self.log.log(self.fileObject, "Deleting Good Data Folder!!!")
            # self.raw_data.deleteExistingGoodDataTrainingFolder()
            # self.log.log(self.fileObject, "Good_Data folder deleted!!!")
            # self.log.log(self.fileObject, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # self.raw_data.moveBadFilesToArchiveBad()
            # self.log.log(self.fileObject, "Bad files moved to archive!! Bad folder Deleted!!")
            # self.log.log(self.fileObject, "Validation Operation completed!!")
            # self.log.log(self.fileObject, "Extracting csv file from table")
            # self.dbOperations.selectingDatafromtableintocsv('Training')
            # self.log.log(self.fileObject, "Successfully exported data from db to csv")
            # self.fileObject.close()
        except Exception as e:
            raise e


        
