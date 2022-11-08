import pandas as pd
import numpy as np
from app_logger.logger import APP_LOGGER
from sklearn.impute import KNNImputer
from feature_engine.imputation import CategoricalImputer
from scipy import stats
import matplotlib.pyplot as plt
import os
class Preprocesser:
    def __init__(self,file_Object):
        self.log=APP_LOGGER()
        self.file_object=file_Object
    
    def remove_Columns(self,data,columns):
        """
            Method Name: remove_Columns
            Description: This method is to remove the columns from the pandas dataframes
            Output:  A pandas DataFrame after removing the specified columns   
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object,'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1)
            self.log.log(self.file_object,'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.log.log(self.file_object,'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise e
    
    
    def separate_label_feature(self, data, label_column_name):
        """
            Method Name: separate_label_feature
            Description: This method is to separate the features and a Label Coulmns.
            Output:  Returns two separate Dataframes, one containing features and the other containing Labels
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1)
            self.Y=data[label_column_name]
            self.log.log(self.file_object,'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()
    def dropUnnecessaryColumns(self,data,columnNameList):
        """
            Method Name: dropUnnecessaryColumns
            Description: This method is to drop unnecessary column values
            Output:  returns the data frame with the removed columns
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        data = data.drop(columnNameList,axis=1)
        return data

    def getTrainDataColumns(self):
        """
            Method Name: deleteColumnsBasedonTrain
            Description: This method is to get the column names from train file and remove the columns based on this
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            self.log.log(self.file_object,'Entered the getTrainDataColumns method of the Preprocessor class')
            for file in os.listdir('Prediction_Raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Prediction_Raw_files_validated/Good_Raw/'+file)
                train_columns=csv.columns
            return train_columns       
        except Exception as e:
            self.log.log(self.file_object, "Error Occured:: %s" % e)
            raise e

    def get_Uncommon_Columns(self,list1,list2):
        """
            Method Name: get_Uncommon_Columns
            Description: This method is to get the column names from train file and predit file which are not common
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            self.log.log(self.file_object,'Entered the get_Uncommon_Columns method of the Preprocessor class')
            col_to_remove =list(set(list1)-set(list2))
            return col_to_remove        
        except Exception as e:
            self.log.log(self.file_object, "Error Occured:: %s" % e)
            raise e


    def replaceInvalidValuesWithNull(self,data):
        """
            Method Name: replaceInvalidValuesWithNull
            Description: This method is to replace invalid values with null
            Output:  returns the data frame with the removed invalid values
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        for column in data.columns:
            count = data[column][data[column] == '?'].count()
            if count != 0:
                data[column] = data[column].replace('?', np.nan)
        return data

    def isNull_Present(self,data):
        """
            Method Name: isNull_Present
            Description: This method is check if the null values are present
            Output:  Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        self.cols_with_missing_values=[]
        self.cols = data.columns
        try:
            self.null_counts=data.isna().sum()
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present=True
                    self.cols_with_missing_values.append(self.cols[i])
            if(self.null_present): 
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') 
            self.log.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def getColumns_with_NAZero(self,data):
        """
            Method Name: getColumns_with_NAZero
            Description: This method is check if the null values are present or zero values are present for the column and get all those column names
            Output:  Returns list of column names which has na or zero values
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the getColumns_with_NAZero method of the Preprocessor class')
        self.allzero_colms =[]
        self.null_present=False
        try:
            for column in data:
                if data[column].isna().all():
                    self.allzero_colms.append(column)
                    self.null_present=True
                        #print(allzero_colms)
            if(self.null_present): 
                self.dataframe_with_nazero = pd.DataFrame()
                self.dataframe_with_nazero['columns'] = data.columns
                self.dataframe_with_nazero['zero values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_nazero.to_csv('preprocessing_data/nazero_values.csv') 
            self.log.log(self.file_object,'Finding zero or na values is a success.Data written to the null values file. Exited the getColumns_with_NAZero method of the Preprocessor class')
            return self.null_present, self.allzero_colms
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in getColumns_with_NAZero method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding missing values failed. Exited the getColumns_with_NAZero method of the Preprocessor class')
            raise Exception()

    def dropColumns_with_Constant(self,data):
        """
            Method Name: dropColumns_with_Constant
            Description: This method is check if the constant values are present for the column and drop all those column names
            Output:  data without constant values
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the dropColumns_with_Constant method of the Preprocessor class')
        try:
            data=data.drop(data.columns[data.nunique()==1],axis=1)
            self.log.log(self.file_object,'Finding constant values is a success.Data written to the constant values file. Exited the dropColumns_with_Constant method of the Preprocessor class')
            return data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in dropColumns_with_Constant method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding constant values failed. Exited the dropColumns_with_Constant method of the Preprocessor class')
            raise Exception()

    def encodeCategoricalValues(self,data):
        """
            Method Name: encodeCategoricalValues
            Description: This method encodes all the categorical values in the data set
            Output:  A Dataframe which has all the categorical values encoded
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the encodeCategoricalValues method of the Preprocessor class')
        try:
            data["class"] = data["class"].map({'p': 1, 'e': 2})
            for column in data.drop(['class'],axis=1).columns:
                data = pd.get_dummies(data, columns=[column])
            self.log.log(self.file_object,'encoding the categorical values is a success.Data written to the constant values file. Exited the encodeCategoricalValues method of the Preprocessor class')
            return data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in encodeCategoricalValues method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding categorical columns failed. Exited the encodeCategoricalValues method of the Preprocessor class')
            raise Exception()

    def drop_Features_with_Coorelation(self,data):
        """
            Method Name: drop_Features_with_Coorelation
            Description: This method drops columns which has coleration with features
            Output:  A Dataframe which has all the correlation features dropped
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the drop_Features_with_Coorelation method of the Preprocessor class')
        try:
            cor_matrix = data.corr().abs()
            upper_tri = cor_matrix.where(np.triu(np.ones(cor_matrix.shape),k=1).astype(np.bool))
            to_drop = [column for column in upper_tri.columns if any(upper_tri[column] > 0.95)]
            data = data.drop(to_drop, axis=1)
            return data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in drop_Features_with_Coorelation method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding correlation features failed. Exited the drop_Features_with_Coorelation method of the Preprocessor class')
            raise Exception()

    def remove_outliers(self,data):
        """
            Method Name: remove_outliers
            Description: This method is to remove the outliers
            Output:  A Dataframe without outliers
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the remove_outliers method of the Preprocessor class')
        try:
            z=np.abs(stats.zscore(data))
            z_p=data[(z < 3).all(axis=1)]
            Q1=data.quantile(0.25)
            Q3=data.quantile(0.75)
            IQR=Q3-Q1
            lowqe_bound=Q1 - 1.5 * IQR
            upper_bound=Q3 + 1.5 * IQR
            IQR_p = data[~((data < lowqe_bound) |(data > upper_bound)).any(axis=1)]
            data=pd.DataFrame(IQR_p)
            return data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in remove_outliers method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Finding outliers failed. Exited the remove_outliers method of the Preprocessor class')
            raise Exception()


    def impute_missing_values(self,data,cols_with_missing_values):
        """
            Method Name: impute_missing_values
            Description: This method replaces all the missing values in the Dataframe using KNN Imputer
            Output:  A Dataframe which has all the missing values imputed
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        self.cols_with_missing_values=cols_with_missing_values
        try:
            self.imputer = CategoricalImputer()
            for col in self.cols_with_missing_values:
                self.data[col] = self.imputer.fit_transform(self.data[col])
                self.log.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            imputer=KNNImputer(n_neighbors=3,weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data)
            self.new_data=pd.DataFrame(data=self.new_array,columns=self.data.columns)
            self.log.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def get_columns_with_zero_std_deviation(self,data):
        """
            Method Name: get_columns_with_zero_std_deviation
            Description: This method finds out the columns which have a standard deviation of zero
            Output:  List of the columns with standard deviation of zero
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                 if (self.data_n[x]['std'] == 0):
                    self.col_to_drop.append(x) 
            self.log.log(self.file_object, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop
        except Exception as e:
            self.log.log(self.file_object,'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.log.log(self.file_object, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()

