import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from app_logger.logger import APP_LOGGER
from File_Operations.fileMethods import File_Operation
class KMeansClustering:
    def __init__(self,file_Object):
        self.file_Object=file_Object
        self.log=APP_LOGGER()
    
    def elbow_plot(self,data):
        """
            Method Name: elbow_plot
            Description: This method is saved the plot to decide the optimum number of clusters to the file
            Output:  A picture saved to the directory   
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_Object, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss=[] 
        try:
            for i in range (1,15):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=74) 
                kmeans.fit(data) 
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,15),wcss) 
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG') 
            self.kn = KneeLocator(range(1, 15), wcss, curve='convex', direction='decreasing')
            self.log.log(self.file_Object, f'The optimum number of clusters is: {self.kn.knee} . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee
        except Exception as e:
            self.log.log(self.file_Object,'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.log.log(self.file_Object,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()


    def create_clusters(self,data,number_of_clusters):
        """
            Method Name: create_clusters
            Description: This method is to Create a new dataframe consisting of the cluster information
            Output: A datframe with cluster column  
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        self.log.log(self.file_Object, 'Entered the create_clusters method of the KMeansClustering class')
        self.data=data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            self.y_kmeans=self.kmeans.fit_predict(data)
            self.file_op = File_Operation(self.file_Object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') 
            self.data['Cluster']=self.y_kmeans 
            self.log.log(self.file_Object, 'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data
        except Exception as e:
            self.log.log(self.file_Object,'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.log.log(self.file_Object,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()

