from Data_Preprocessing.preprocessing import Preprocesser
from app_logger.logger import APP_LOGGER
from Training_Data_Ingestion.trainDataLoader import Data_Getter
from Data_Preprocessing.clustering import KMeansClustering
from sklearn.model_selection import train_test_split
from Best_Model_Finder.tuner import Model_Finder
from File_Operations.fileMethods import File_Operation
class Model_Training:
    def __init__(self):
        self.log=APP_LOGGER()
        self.file_object = open("Log_Files_Collection/Training_Logs/ModelTrainingLog.txt", 'a+')
    def trainingModel(self):
        """
            Method Name: trainingModel
            Description: This method is for training model
            Output:  None   
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_object, 'Start of Training')
        try:
            data_getter=Data_Getter(self.file_object)
            data=data_getter.get_data()
            self.log.log(self.file_object, 'the data is loaded successfully')
            preprocessor=Preprocesser(self.file_object)
            #data = preprocessor.dropUnnecessaryColumns(data,['veiltype'])
            is_zerona_present,cols_with_nazerovalues=preprocessor.getColumns_with_NAZero(data)
            if(is_zerona_present):
                data=preprocessor.remove_Columns(data,cols_with_nazerovalues)
            data = preprocessor.replaceInvalidValuesWithNull(data)
            is_null_present,cols_with_missing_values=preprocessor.isNull_Present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data,cols_with_missing_values)
            #data = preprocessor.encodeCategoricalValues(data)
           # data=preprocessor.dropColumns_with_Constant(data)
            data=preprocessor.drop_Features_with_Coorelation(data)
           # data=preprocessor.remove_outliers(data)
            X,Y=preprocessor.separate_label_feature(data,label_column_name='phishing')
            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(X)
            X=preprocessor.remove_Columns(X,cols_to_drop)
            self.log.log(self.file_object, f'The column names folr training data are {X.columns}')
            kmeans=KMeansClustering(self.file_object)
            number_of_clusters=kmeans.elbow_plot(X)
            X=kmeans.create_clusters(X,number_of_clusters)
            X['phishing']=Y
            list_of_clusters=X['Cluster'].unique()
            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i]
                cluster_features=cluster_data.drop(['phishing','Cluster'],axis=1)
                cluster_label= cluster_data['phishing']
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=36)
                model_finder=Model_Finder(self.file_object)
                best_model_name,best_model=model_finder.get_best_model(x_train,y_train,x_test,y_test)
                file_op = File_Operation(self.file_object)
                save_model=file_op.save_model(best_model,best_model_name+str(i))
            self.log.log(self.file_object, 'Successful End of Training')
            self.file_object.close()
        except Exception:
            self.log.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception