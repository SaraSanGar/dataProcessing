from flask import Flask, jsonify, request
from datetime import datetime
from flask_cors import CORS
import pandas as pd
import os

app = Flask(__name__)
CORS(app)

fileName = None

def fileDate():
    fileNameDate = datetime.strptime(fileName, "%Y%m%dT%H%M%SZ")
    date = fileNameDate.strftime("%d-%m-%Y")
    return date

def detectFiles():
    files = []
    path = './rawData/'
    for element in os.listdir(path):
        nameElement = os.path.splitext(element)[0]
        files.append(nameElement)
    return files

def mapData(overview_df):
    map_df = pd.DataFrame()
    map_df['id'] = overview_df['Id']
    map_df['lat'] = overview_df['lat[deg]']
    map_df['lon'] = overview_df['lon[deg]']
    return map_df

def detectionsData(overview_df):
    deteccions_df = pd.DataFrame()
    deteccions_df['id'] = overview_df['Id']
    deteccions_df['label'] = overview_df['label']
    deteccions_df['depth'] = overview_df['depth[m]']
    return deteccions_df

def descriptorsData(overview_df):
    descriptors_columns = ['Descriptor', 'Media', 'Sdev', 'Mediana', 'Percentil10', 'Percentil90']
    descriptors_df = pd.DataFrame(columns=descriptors_columns)

    all_descriptors = ['depth[m]', 'clip[usec]', 'msec', 'low_i', 'high_i', 'energy[%]', 'SNR[dB]','Fp[kHz]', 'Fc[kHz]', 'RMS[kHz]', 'Qrms', 'Fs[kHz]', 'slope_max_wvd']
    for descriptor in all_descriptors:
        descriptor_data = [descriptor, round(overview_df[descriptor].mean(), 2), round(overview_df[descriptor].std(), 2), round(overview_df[descriptor].median(), 2), round(overview_df[descriptor].quantile(.1), 2), round(overview_df[descriptor].quantile(.9), 2)]
        descriptors_df.loc[len(descriptors_df)] = descriptor_data
    return descriptors_df

def spectogramData(overview_df):
    spectogram_columns = ['Data', 'Promedio', 'Sdev', 'Mediana']
    spectogram_df = pd.DataFrame(columns=spectogram_columns)

    all_descriptors = ['PosixTime', 'energy[%]']
    for column in all_descriptors:
        spectogram_data = [column, round(overview_df[column].mean(), 2), round(overview_df[column].std(), 2), round(overview_df[column].median(), 2)]
        spectogram_df.loc[len(spectogram_df)] = spectogram_data
    return spectogram_df

def processData(fileName):
    overview_df = pd.read_csv('./rawData/'+fileName+'.csv')

    maps_df = mapData(overview_df)
    deteccions_df = detectionsData(overview_df)
    descriptors_df = descriptorsData(overview_df)
    spectogram_df = spectogramData(overview_df)

    os.mkdir('./processedData/processed'+fileName)
    maps_df.to_csv('./processedData/processed'+fileName+'/map'+fileName+'.csv', index=False)
    deteccions_df.to_csv('./processedData/processed'+fileName+'/deteccions'+fileName+'.csv', index=False)
    descriptors_df.to_csv('./processedData/processed'+fileName+'/descriptors'+fileName+'.csv', index=False)
    spectogram_df.to_csv('./processedData/processed'+fileName+'/spectogram'+fileName+'.csv', index=False)

def openMap(fileName):
    with open('./processedData/processed'+fileName+'/map'+fileName+'.csv', 'r') as file:
        return file.read()
    
def openDeteccions(fileName):
    with open('./processedData/processed'+fileName+'/deteccions'+fileName+'.csv', 'r') as file:
        return file.read()

def openDescriptors(fileName):
    with open('./processedData/processed'+fileName+'/descriptors'+fileName+'.csv', 'r') as file:
        return file.read()
    
def openSpectogram(fileName):
    with open('./processedData/processed'+fileName+'/spectogram'+fileName+'.csv', 'r') as file:
        return file.read()

@app.route("/processedData/map")
def map_csv(): 
    map_csv = openMap(fileName)
    return jsonify(map_csv), 200

@app.route("/processedData/deteccions")
def deteccions_csv(): 
    deteccions_csv = openDeteccions(fileName)
    return jsonify(deteccions_csv), 200

@app.route("/processedData/descriptors")
def descriptors_csv(): 
    descriptors_csv = openDescriptors(fileName)
    return jsonify(descriptors_csv), 200

@app.route("/processedData/spectogram")
def spectogram_csv(): 
    spectogram_csv = openSpectogram(fileName)
    return jsonify(spectogram_csv), 

@app.route("/processedData/files")
def files(): 
    files = detectFiles()
    return jsonify(files), 200

@app.route("/processedData/", methods=['POST'])
def procesar_datos():
    global fileName
    data = request.get_json()
    fileName = data['fileName']

    date = fileDate();
    
    if not os.path.exists('./processedData/processed' + fileName):
        processData(fileName)
    
    return jsonify(fileName=fileName, date=date), 200

if __name__ == '__main__':
    app.run(debug=True)
