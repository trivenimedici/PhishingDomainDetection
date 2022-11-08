from flask import Flask ,request, render_template,Response
from wsgiref import simple_server
import flask_monitoringdashboard as dashboard
import os
from flask_cors import CORS, cross_origin
import json
from Training_Validations.trainingValidation import train_validation
from Prediction_Validations.predictionValidation import pred_validation
from Training_Model.trainingModel import Model_Training
from Prediction_Model.predictFromModel import prediction
from URL_Extraction.ExtractURLProperties import  extract_url_properties
import shutil

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/",methods=['POST','GET'])
@cross_origin()
def home():
    try:
        if request.method == 'POST':
            url = request.form['content']
            user_data=extract_url_properties(url)
            user_data.createResultDir()
            input_data_path=user_data.load_user_data()
            if input_data_path == 'invalid url':
                return render_template('error.html')
            else:
                pred_val = pred_validation(input_data_path)
                pred_val.pred_validation()
                pred= prediction(input_data_path)
                result = pred.validateURLFromModel()
                if result==0:
                    res='It Looks like a legitimate  domain!!'
                else:
                    res='Its Looks like a Phishing domain!! Be cautious to access '
                if os.path.isdir('Result_Dataset_File/input_data/'):
                    shutil.rmtree('Result_Dataset_File/input_data/')
                return render_template('results.html',prediction=res)
        else:
            return render_template('index.html')
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)

@app.route("/train", methods=['POST', 'GET'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path=request.json['folderPath']
            train_val = train_validation(path)
            train_val.train_validation()
            train_model= Model_Training()
            train_model.trainingModel()
        elif request.form is not None:
            path = request.form['filepath']
            train_val = train_validation(path)
            train_val.train_validation()
            train_model= Model_Training()
            train_model.trainingModel()
        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occurred! %s" % ValueError) 
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")



@app.route("/predict",methods=['POST', 'GET'])
@cross_origin()
def predictRountClient():
    try:
        if request.json is not None:
            path=request.json['filepath']
            pred_val = pred_validation(path)
            pred_val.pred_validation()
            pred= prediction(path)
            path,json_predictions = pred.predictionFromModel()
            return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions) ))
        elif request.form is not None:
            path = request.form['filepath']
            pred_val = pred_validation(path)
            pred_val.pred_validation()
            pred= prediction(path)
            path,json_predictions = pred.predictionFromModel()
            return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions) ))
        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)

# @app.route("/result",methods=['POST', 'GET'])
# @cross_origin()
# def showResult():
#     try:
#         if request.method == 'POST':
#             url = request.form['websiteurl']
#             user_data=extract_url_properties(url)
#             input_data_path=user_data.load_user_data()
#             if input_data_path == 'invalid url':
#                 return render_template('error.html')
#             else:
#                 pred_val = pred_validation(input_data_path)
#                 pred_val.pred_validation()
#                 pred= prediction(path)
#                 path,json_predictions,result = pred.predictionFromModel()
#                 if result == 0:
#                     res='It Looks like a legitimate  website!!'
#                 else:
#                     res='Its Looks like a Phishing Website!! Be cautious to access '
#                 return render_template('results.html')
#         else:
#             return render_template('index.html')
#     except ValueError:
#         return Response("Error Occurred! %s" %ValueError)
#     except KeyError:
#         return Response("Error Occurred! %s" %KeyError)
#     except Exception as e:
#         return Response("Error Occurred! %s" %e)
port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    # host = '0.0.0.0'
    # httpd = simple_server.make_server(host, port, app)
    # httpd.serve_forever()
    app.run(debug=True)