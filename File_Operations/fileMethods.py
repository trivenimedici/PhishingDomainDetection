import pickle
import os
import shutil
from app_logger.logger import APP_LOGGER

class File_Operation:
    def __init__(self,file_Object):
        self.file_Object=file_Object
        self.log=APP_LOGGER()
        self.model_directory='models/'

    def save_model(self,model,filename):
        """
            Method Name: save_model
            Description: This method is to Save the model file to directory
            Output:  File gets saved
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_Object, 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory,filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path +'/' + filename+'.sav','wb') as f:
                pickle.dump(model, f)
                self.log.log(self.file_Object,'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')
            return 'success'
        except Exception as e:
            self.log.log(self.file_Object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.log.log(self.file_Object,'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()
    
    def load_model(self,filename):
        """
            Method Name: load_model
            Description: This method is to load the model file to memory
            Output:  Model file loaded in memory
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_Object, 'Entered the load_model method of the File_Operation class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav','rb') as f:
                self.log.log(self.file_Object,'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
        except Exception as e:
            self.log.log(self.file_Object,'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.log.log(self.file_Object,'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()

    def find_correct_model_file(self,cluster_number):
        """
            Method Name: load_model
            Description: This method is to Select the correct model based on cluster number
            Output:  Model file
            On Failure: Raise Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_Object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.log.log(self.file_Object,'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.log.log(self.file_Object,
                                   'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.log.log(self.file_Object,
                                   'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise Exception()
