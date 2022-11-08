from datetime import datetime
import os
class APP_LOGGER:
    def __init__(self) -> None:
        pass

    def log(self,fileObject,logMessage):
        self.now = datetime.now()
        self.current_date=self.now.date()
        self.current_time=self.now.strftime('%H:%M:%S')
        fileObject.write(str(self.current_date)+"_"+str(self.current_time)+"\t\t"+logMessage+"\n")

    def createLoggerFile(self,fileName):
        try:
            file_open=None
            file_path="Log_Files_Collection/Training_Logs/"+fileName
            if not os.path.exists(file_path):
                file_open= open(file_path,"a+",encoding='utf-8')
            return file_open
        except OSError:
            file=open("ErrorLogs.txt","a+",encoding='utf-8')
            self.log(file,"Error while creating log file for "+fileName+"%s:" % OSError)
            raise OSError

    def deleteExistingLogFiles(self,dir_path):
        try:
            for f in os.listdir(dir_path):
                file =os.path.join(dir_path,f)
                if os.path.isfile(file):
                    print('Deleting file:', file)
                    os.remove(file)
        except OSError:
            file=open("ErrorLogs.txt","a+",encoding='utf-8')
            self.log(file,"Error while deleteing log file for %s:" % OSError)
            raise OSError


