from flask import Flask, jsonify, request
from datetime import datetime
from flask_cors import CORS
import pandas as pd
import numpy as np
import wave
import os

app = Flask(__name__)
CORS(app)

fileName = None

def fileDate():
    fileNameDate = datetime.strptime(fileName, "%Y%m%dT%H%M%SZ")
    date = fileNameDate.strftime("%d-%m-%Y")
    return date

def fileLabels():
    overview_df = pd.read_csv('./rawData/'+fileName+'.csv')
    labels = overview_df['label'].unique().tolist()
    return labels

def detectFiles():
    files = []
    path = './rawData/'
    for element in os.listdir(path):
        nameElement = os.path.splitext(element)[0]
        files.append(nameElement)
    return files

def getDetectionData(detectionName):
    data = pd.read_csv('./rawData/'+fileName+'.csv')
    if isinstance(detectionName, int):
        detectionName = str(detectionName)
    filtered_data = data[data['Id'].astype(str).str.strip() == detectionName.strip()]
    deteccion_df = pd.DataFrame()
    all_descriptors = ['Id', 'state','date', 'time', 'label','depth[m]', 'clip[usec]', 'msec','energy[%]', 'SNR[dB]','Fp[kHz]', 'Fc[kHz]', 'RMS[kHz]', 'Qrms', 'Fs[kHz]', 'slope_max_wvd', 'lat[deg]', 'lon[deg]']
    for descriptor in all_descriptors:
        if filtered_data[descriptor].empty:
            filtered_data[descriptor] = 0
        deteccion_df[descriptor] = filtered_data[descriptor]
        
    return deteccion_df

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
    all_descriptors = ['depth[m]', 'clip[usec]', 'msec', 'low_i', 'high_i', 'energy[%]', 'SNR[dB]','Fp[kHz]', 'Fc[kHz]', 'RMS[kHz]', 'Qrms', 'Fs[kHz]', 'slope_max_wvd']
    dfs_per_label = {}
    for label, group in overview_df.groupby('label'):
        descriptors_df = pd.DataFrame(columns=descriptors_columns)
        for descriptor in all_descriptors:
            descriptor_data = [descriptor, 
                               round(group[descriptor].mean(), 2), 
                               round(group[descriptor].std(), 2), 
                               round(group[descriptor].median(), 2), 
                               round(group[descriptor].quantile(.1), 2), 
                               round(group[descriptor].quantile(.9), 2)]
            descriptors_df.loc[len(descriptors_df)] = descriptor_data
        dfs_per_label[label] = descriptors_df
    return dfs_per_label

def spectrumData(archivo_wav):
    with wave.open(archivo_wav, 'rb') as wav_file:
        num_frames = wav_file.getnframes()
        framerate = wav_file.getframerate()
        duration = num_frames / framerate
        frames = wav_file.readframes(num_frames)

    datos = np.frombuffer(frames, dtype=np.int16).tolist()
    return {
        "datos": datos,
        "duration": duration,
        "num_frames": num_frames
    }

def processData(fileName):
    overview_df = pd.read_csv('./rawData/'+fileName+'.csv')
    os.mkdir('./processedData/processed'+fileName)

    maps_df = mapData(overview_df)
    maps_df.to_csv('./processedData/processed'+fileName+'/map'+fileName+'.csv', index=False)

    deteccions_df = detectionsData(overview_df)
    deteccions_df.to_csv('./processedData/processed'+fileName+'/deteccions'+fileName+'.csv', index=False)

    dfs_per_label = descriptorsData(overview_df)
    for label in dfs_per_label:
        descriptors_df = dfs_per_label[label]
        descriptors_df.to_csv('./processedData/processed'+fileName+'/descriptors'+fileName+label+'.csv', index=False)

def openMap(fileName):
    with open('./processedData/processed'+fileName+'/map'+fileName+'.csv', 'r') as file:
        return file.read()
    
def openDeteccions(fileName):
    with open('./processedData/processed'+fileName+'/deteccions'+fileName+'.csv', 'r') as file:
        return file.read()

def openDescriptors(fileName, label):
    with open('./processedData/processed'+fileName+'/descriptors'+fileName+label+'.csv', 'r') as file:
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
    descriptors_csv = ""
    label = request.args.get('label')
    if label !="":
        descriptors_csv = openDescriptors(fileName, label)
    return jsonify(descriptors_csv), 200

@app.route("/processedData/spectrum")
def spectrum_csv(): 
    spectrum_df, duration, num_frames = spectrumData('./wavData/'+fileName+'.wav')

    return jsonify(spectrum_df, duration, num_frames), 200

@app.route("/processedData/files")
def files(): 
    files = detectFiles()
    return jsonify(files), 200

@app.route("/processedDetection/all_detections")
def detections(): 
    global fileName
    data = pd.read_csv('./rawData/'+fileName+'.csv')
    detections_names = data['Id'].tolist()
    return jsonify(detections_names), 200

@app.route("/processedDetection", methods=['POST'])
def processed_detectios():
    data = request.get_json()
    detectionName = data['detectionName']
    detectionData = getDetectionData(detectionName).to_dict(orient='records')[0]
    date = detectionData['date']
    time = detectionData['time']
    
    return jsonify(fileName=fileName, date=date, time=time, detectionData=detectionData), 200

@app.route("/processedData/", methods=['POST'])
def processed_data():
    global fileName
    data = request.get_json()
    fileName = data['fileName']

    if not fileName:
        return jsonify(error="No se proporcionó ningún nombre de archivo."), 400

    date = fileDate()
    labels = fileLabels()

    if not os.path.exists('./processedData/processed' + fileName):
        print("Se han seleccionado datos sin procesar")
        processData(fileName)
    
    return jsonify(fileName=fileName, date=date, labels=labels), 200

if __name__ == '__main__':
    app.run(debug=True)
