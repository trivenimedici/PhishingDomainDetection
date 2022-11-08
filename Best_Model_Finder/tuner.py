from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from app_logger.logger import APP_LOGGER
from sklearn.metrics import accuracy_score, confusion_matrix, roc_curve, roc_auc_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model  import  LogisticRegression
from sklearn.naive_bayes import GaussianNB

class Model_Finder:
    def __init__(self,file_object):
        self.file_object = file_object
        self.logger_object = APP_LOGGER()
        self.sv_classifier=SVC()
        self.xgb = XGBClassifier(objective='binary:logistic',n_jobs=-1)
    def get_best_params_for_svm(self,train_x,train_y):
        """
            Method Name: get_best_params_for_svm
            Description: get the parameters for the SVM Algorithm which give the best accuracy
            Output:  The model with the best parameters   
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')
        try:
            self.param_grid = {"kernel": ['rbf', 'sigmoid'],"C": [0.1, 0.5, 1.0],"random_state": [0, 100, 200, 300]}
            self.grid = GridSearchCV(estimator=self.sv_classifier, param_grid=self.param_grid, cv=5,  verbose=3)
            self.grid.fit(train_x, train_y)
            self.kernel = self.grid.best_params_['kernel']
            self.C = self.grid.best_params_['C']
            self.random_state = self.grid.best_params_['random_state']
            self.sv_classifier = SVC(kernel=self.kernel,C=self.C,random_state=self.random_state)
            self.sv_classifier.fit(train_x, train_y)
            self.logger_object.log(self.file_object,'SVM best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_svm method of the Model_Finder class')
            return self.sv_classifier
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_svm method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'SVM training  failed. Exited the get_best_params_for_svm method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_xgboost(self,train_x,train_y):
        """
            Method Name: get_best_params_for_xgboost
            Description: get the parameters for XGBoost Algorithm which give the best accuracy
            Output:  The model with the best parameters   
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.logger_object.log(self.file_object,'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            self.param_grid_xgboost = {"n_estimators": [100, 130], "criterion": ['gini', 'entropy'],"max_depth": range(8, 10, 1)}
            self.grid= GridSearchCV(XGBClassifier(objective='binary:logistic'),self.param_grid_xgboost, verbose=3,cv=5)
            self.grid.fit(train_x, train_y)
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.xgb = XGBClassifier(criterion=self.criterion, max_depth=self.max_depth,n_estimators= self.n_estimators, n_jobs=-1 )
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object,'XGBoost best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()
    def get_best_model(self,train_x,train_y,test_x,test_y):
        """
            Method Name: get_best_model
            Description: Find out the Model which has the best AUC score
            Output:  The best model name and the model object
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.logger_object.log(self.file_object,'Entered the get_best_model method of the Model_Finder class')
        try:
            self.logr_liblinear = LogisticRegression(verbose=1,solver='liblinear')
            self.logr_liblinear=self.logr_liblinear.fit(train_x,train_y)
            self.logr_liblinear.feature_names=list(train_x.columns.values)
            self.prediction_logistic= self.logr_liblinear.predict(test_x)
            if len(test_y.unique()) == 1:
                self.logistic_score = accuracy_score(test_y, self.prediction_logistic)
                self.logger_object.log(self.file_object, 'Accuracy for Logistic:' + str(self.logistic_score)) 
            else:
                self.logistic_score = roc_auc_score(test_y, self.prediction_logistic) 
                self.logger_object.log(self.file_object, 'AUC for Logistic:' + str(self.logistic_score)) 
            self.decision_tree = DecisionTreeClassifier()
            self.decision_tree=self.decision_tree.fit(train_x,train_y)
            self.decision_tree.feature_names=list(train_x.columns.values)
            self.prediction_destree= self.decision_tree.predict(test_x)
            if len(test_y.unique()) == 1:
                self.desctree_score = accuracy_score(test_y, self.prediction_destree)
                self.logger_object.log(self.file_object, 'Accuracy for Descision Tree:' + str(self.desctree_score)) 
            else:
                self.desctree_score = roc_auc_score(test_y, self.prediction_destree) 
                self.logger_object.log(self.file_object, 'AUC for Descision Tree:' + str(self.desctree_score)) 
            self.naive_Bayes = GaussianNB()
            self.naive_Bayes=self.naive_Bayes.fit(train_x,train_y)
            self.naive_Bayes.feature_names=list(train_x.columns.values)
            self.prediction_naive_Bayes= self.naive_Bayes.predict(test_x)
            if len(test_y.unique()) == 1:
                self.naive_Bayes_score = accuracy_score(test_y, self.prediction_naive_Bayes)
                self.logger_object.log(self.file_object, 'Accuracy for naive Bayes :' + str(self.naive_Bayes_score)) 
            else:
                self.naive_Bayes_score = roc_auc_score(test_y, self.prediction_naive_Bayes) 
                self.logger_object.log(self.file_object, 'AUC for naive Bayes:' + str(self.naive_Bayes_score)) 
            self.xgboost= self.get_best_params_for_xgboost(train_x,train_y)
            self.xgboost.feature_names=list(train_x.columns.values)
            self.prediction_xgboost = self.xgboost.predict(test_x)
            if len(test_y.unique()) == 1:
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score)) 
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost) 
                self.logger_object.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score)) 
            # self.svm=self.get_best_params_for_svm(train_x,train_y)
            # self.svm.feature_names=list(train_x.columns.values)
            # self.prediction_svm=self.svm.predict(test_x) 
            # if len(test_y.unique()) == 1:
            #     self.svm_score = accuracy_score(test_y,self.prediction_svm)
            #     self.logger_object.log(self.file_object, 'Accuracy for SVM:' + str(self.svm_score))
            # else:
            #     self.svm_score = roc_auc_score(test_y, self.prediction_svm) 
            #     self.logger_object.log(self.file_object, 'AUC for SVM:' + str(self.svm_score))
           # all_models ={"SVM":self.svm,"XGBoost":self.xgboost,"Naive Bayes":self.naive_Bayes,"Desicision Tree":self.decision_tree,"Logistic":self.logr_liblinear}
            #all_models_scores ={"SVM":self.svm_score,"XGBoost":self.xgboost_score,"Naive Bayes":self.naive_Bayes_score,"Desicision Tree":self.desctree_score,"Logistic":self.logistic_score}
            all_models ={"XGBoost":self.xgboost,"Naive Bayes":self.naive_Bayes,"Desicision Tree":self.decision_tree,"Logistic":self.logr_liblinear}
            all_models_scores ={"XGBoost":self.xgboost_score,"Naive Bayes":self.naive_Bayes_score,"Desicision Tree":self.desctree_score,"Logistic":self.logistic_score}
           # best_model_score=max(all_models_scores.values())
            best_model_name=max(all_models_scores, key=all_models_scores.get)
            best_model=all_models[best_model_name]
            return best_model_name,best_model         
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()

