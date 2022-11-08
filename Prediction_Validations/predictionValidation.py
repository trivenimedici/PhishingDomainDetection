from Prediction_Raw_Data_Validations.predRawValidations import RawValidations
from app_logger.logger import APP_LOGGER
from Prediction_DataTransformations.predDataTransformations import Data_Transformations
from PredictionDataInsertion_To_DB.predDataInsertionToDB import Data_Insertion_ToDB
class pred_validation:
    def __init__(self,path):
        self.raw_data=RawValidations(path)
        self.fileObject=open("Log_Files_Collection/Prediction_Logs/Prediction_Main_Log.txt","a+")
        self.log=APP_LOGGER()
        self.dbOperations=Data_Insertion_ToDB
        self.data_transform=Data_Transformations()

    def pred_validation(self):
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
            self.log.log(self.fileObject,"Creating Prediction_Database and tables on the basis of given schema!!!")
            # self.dbOperations.createTableDb('Prediction',col_names)
            # self.log.log(self.fileObject, "Table creation Completed!!")
            # self.log.log(self.fileObject, "Insertion of Data into Table started!!!!")
            # self.dbOperations.insertIntoTableGoodData('Prediction')
            # self.log.log(self.fileObject, "Insertion in Table completed!!!")
            # self.log.log(self.fileObject, "Deleting Good Data Folder!!!")
            # self.raw_data.deleteExistingGoodDataPredictionFolder()
            # self.log.log(self.fileObject, "Good_Data folder deleted!!!")
            # self.log.log(self.fileObject, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # self.raw_data.moveBadFilesToArchiveBad()
            # self.log.log(self.fileObject, "Bad files moved to archive!! Bad folder Deleted!!")
            # self.log.log(self.fileObject, "Validation Operation completed!!")
            # self.log.log(self.fileObject, "Extracting csv file from table")
            # self.dbOperations.selectingDatafromtableintocsv('Prediction')
            # self.log.log(self.fileObject, "Successfully exported data from db to csv")
            self.fileObject.close()
        except Exception as e:
            raise e


        
