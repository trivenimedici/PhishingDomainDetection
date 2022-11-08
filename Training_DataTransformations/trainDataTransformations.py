from pickle import TRUE
from app_logger.logger import APP_LOGGER
from os import listdir
import pandas as pd
class Data_Transformations:
    def __init__(self):
        self.logger=APP_LOGGER()
        self.good_data_path='Training_Raw_files_validated/Good_Raw'
    
    def replaceMissingWithNull(self):
        """
            Method Name: replaceMissingWithNull
            Description: This method is to replace the missing values in the input dataset with null values
            Output:  None
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        log_file=open("Log_Files_Collection/Training_Logs/dataTransformLog.txt", 'a+')
        try:
            files=[f for f in listdir(self.good_data_path)]
            for f in files:
                self.logger.log(log_file, f"the data transofrmations started for file {f}")
                self.logger.log(log_file, f"the file path is  {self.good_data_path}/{f}")
                csv=pd.read_csv(self.good_data_path+'/'+f)
                csv.fillna('NULL',inplace=True)
                csv.to_csv(self.good_data_path+ "/" + f, index=None, header=True)
                self.logger.log(log_file," %s: File Transformed successfully!!" % f)
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.close()
            raise e
        log_file.close()
