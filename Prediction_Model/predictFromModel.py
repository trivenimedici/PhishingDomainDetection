import pandas 
from File_Operations.fileMethods import File_Operation
from Data_Preprocessing.preprocessing import Preprocesser
from app_logger.logger import APP_LOGGER
from Predicition_Data_Ingestion.predDataLoader import Data_Getter
from Prediction_Raw_Data_Validations.predRawValidations import RawValidations

class prediction:
    def __init__(self,path):
        self.file_object = open("Log_Files_Collection/Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log = APP_LOGGER()
        if path is not None:
            self.pred_data_val = RawValidations(path)
    
    def predictionFromModel(self):
        try:
            self.pred_data_val.deletePredictionFile()
            self.log.log(self.file_object,'Start of Prediction')
            data_getter=Data_Getter(self.file_object)
            data=data_getter.get_data()
            preprocessor=Preprocesser(self.file_object)
            is_zerona_present,cols_with_nazerovalues=preprocessor.getColumns_with_NAZero(data)
            if(is_zerona_present):
                data=preprocessor.remove_Columns(data,cols_with_nazerovalues)
            data = preprocessor.replaceInvalidValuesWithNull(data)
            is_null_present,cols_with_missing_values=preprocessor.isNull_Present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data,cols_with_missing_values)
            #data = preprocessor.encodeCategoricalValues(data)
          #  data=preprocessor.dropColumns_with_Constant(data)
          #  data=preprocessor.drop_Features_with_Coorelation(data)
            data=preprocessor.remove_outliers(data)
            X,Y=preprocessor.separate_label_feature(data,label_column_name='phishing')
       #     X=preprocessor.remove_Columns(X,['qty_and_directory','qty_equal_params'])
           # cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(X)
           # X=preprocessor.remove_Columns(X,cols_to_drop)
            file_loader=File_Operation(self.file_object)
            XGBoost=file_loader.load_model('XGBoost1')
            f_names=XGBoost.feature_names
            self.log.log(self.file_object,f'The column values for the prediction data for xgboost  is {f_names}')
            get_traincoldata=X[f_names]
            clusters=XGBoost.predict(get_traincoldata)
            get_traincoldata['phishing']=Y
            get_traincoldata['cluster']=clusters
            clusters=get_traincoldata['cluster'].unique()
            for i in clusters:
                cluster_data= get_traincoldata[get_traincoldata['cluster']==i]
                Phishing_names = list(cluster_data['phishing'])
                cluster_data=cluster_data.drop(labels=['phishing','cluster'],axis=1)
                self.log.log(self.file_object,f'The column values for the prediction data is {cluster_data.columns}')
              #  cluster_data = cluster_data.drop(['Cluster'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))
                result = pandas.DataFrame(list(zip(Phishing_names,result)),columns=['phishing','Prediction'])
                path="Prediction_Output_File/Predictions.csv"
                result.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+')
            self.log.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, result.head().to_json(orient="records")

    
    def validateURLFromModel(self):
        try:
            self.pred_data_val.deletePredictionFile()
            self.log.log(self.file_object,'Start of Prediction')
            data_getter=Data_Getter(self.file_object)
            data=data_getter.get_data()
            preprocessor=Preprocesser(self.file_object)
            is_zerona_present,cols_with_nazerovalues=preprocessor.getColumns_with_NAZero(data)
            if(is_zerona_present):
                data=preprocessor.remove_Columns(data,cols_with_nazerovalues)
            data = preprocessor.replaceInvalidValuesWithNull(data)
            is_null_present,cols_with_missing_values=preprocessor.isNull_Present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data,cols_with_missing_values)
            X,Y=preprocessor.separate_label_feature(data,label_column_name='phishing')
            file_loader=File_Operation(self.file_object)
            XGBoost=file_loader.load_model('XGBoost1')
            f_names=XGBoost.feature_names
            self.log.log(self.file_object,f'The column values for the prediction data for xgboost  is {f_names}')
            get_traincoldata=X[f_names]
            result=list(XGBoost.predict(get_traincoldata))
            self.log.log(self.file_object,'End of Prediction')
            self.log.log(self.file_object,str(result[0])) 
            return result[0]
        except Exception as ex:
            self.log.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        


        


        
