#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 09:19:21 2022

@author: luca
"""
import io
import pandas as pd
import joblib
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.float_format', '{:.2f}'.format)

def execute(input_connection_pool, output_connection_pool, *params, session_id):

    ## Definisco le connessioni in input
    global kafka_connection, kafka_infos, s3_connection, s3_infos, postgres_connection, postgres_infos
    s3_connection = input_connection_pool["s3"]["connObject"]
    s3_infos = input_connection_pool["s3"]["infos"]

    ## Feature non rilevanti per il modello
    ignored_values = ["timestamp", "cloud_sync", "log_id", "sample_period", "data_start", "press1", "press3", "ctrl_s3",
                      "ctrl_dig0", "ctrl_dig1", "ctrl_dig2"]

    columns = []
    values = []

    ## Leggo i dati raccolti da Clabo da s3
    data = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=s3_infos["input_path"])
    contents = data['Body'].read()
    contents = contents.decode("utf-8").strip().split('\n')

    ## Leggo quelle che saranno le colonne del dataframe
    for line in contents:
        splitted = line.split()
        if splitted[0] not in ignored_values:
            columns.append(splitted[0])
    columns = list(dict.fromkeys(columns))
    df = pd.DataFrame(columns=columns)

    ## Splitto la label dal valore ignorando quelle non rilevanti, appendo ogni riga parsata al dataframe
    for line in contents:
        splitted = line.split()
        if (splitted[0] == "timestamp"):
            if values:
                a_series = pd.Series(values, index=df.columns)
                df = df.append(a_series, ignore_index=True)
                values = []
        if splitted[0] not in ignored_values:
            if (splitted[0] == "timestamp"):
                values.append(splitted[1] + " " + splitted[2])
            else:
                values.append(splitted[1])

    ## Carico il modello da S3
    with io.BytesIO() as file:
        s3_connection.download_fileobj(Bucket=s3_infos["bucket"], Key=s3_infos["model_path"], Fileobj=file)
        file.seek(0)
        loaded_model = joblib.load(file)


    df_renamed = df

    ## Rinomino le colonne preché il modello interpreti correttamente le feature
    df = df.rename(columns={'AIR_SPD_CUST': '218(VDC)', 'LOW_PRS': '302(BAR)', 'HIGH_PRS': '221(BAR)', 'FAN2_TMP': '104(C)',
                       'FAN4_TMP': '102(C)', 'K_INTAKE_TMP': '207(C)', 'K_HEAD_TMP': '208(C)', 'EV2_IN_TMP': '203(C)',
                       'EV2_AIR_OUT_TMP': '114(C)', 'COLDR_TMP': '106(C)', 'EV2_DEFR_TMP': '110(C)',
                       'COND_CTR_TMP': '211(C)', 'RH_CUST': '222(RH)'})

    ## Mantengo un dataframe con le colonne con nomi esplicativi
    df_renamed = df_renamed.rename(columns={'AIR_SPD_CUST': 'AIR_SPD_CUST 218(VDC)', 'LOW_PRS': 'LOW_PRS 302(BAR)', 'HIGH_PRS': 'HIGH_PRS 221(BAR)', 'FAN2_TMP': 'FAN2_TMP 104(C)',
                       'FAN4_TMP': 'FAN4_TMP 102(C)', 'K_INTAKE_TMP': 'K_INTAKE_TMP 207(C)', 'K_HEAD_TMP': 'K_HEAD_TMP 208(C)', 'EV2_IN_TMP': 'EV2_IN_TMP 203(C)',
                       'EV2_AIR_OUT_TMP': 'EV2_AIR_OUT_TMP 114(C)', 'COLDR_TMP': 'COLDR_TMP 106(C)', 'EV2_DEFR_TMP': 'EV2_DEFR_TMP 110(C)',
                       'COND_CTR_TMP': 'COND_CTR_TMP 211(C)', 'RH_CUST': 'RH_CUST 222(RH)'})

    ## Predico sui dati, appendo la colonna delle predizioni al df in output, scrivo il df finale
    result = loaded_model.predict(df)
    df_renamed["prediction"] = result

    ## Scrittura del dataframe in output su filesystem
    
    postgres_connection=output_connection_pool["pg-psycopg2"]["connObject"]
    postgres_infos=output_connection_pool["pg-psycopg2"]["infos"]

    df_renamed.to_csv("output.csv", index=False)
    
    cur=postgres_connection.cursor()
    tabella=postgres_infos["table"]
    with open('output.csv','r') as f:
        next(f)
        cur.copy_from(f, tabella, sep=',') #nome tabella per esec in locale: output
    postgres_connection.commit()
    postgres_connection.close()

    print("execution finished, session id: {}".format(session_id))

    return {"retCode": "OK", "description": "Execution terminated with success"}

