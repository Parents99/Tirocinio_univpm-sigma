#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  4 14:57:53 2022

@author: luca
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 15 13:02:17 2021

@author: luca
"""

import pandas as pd

from sklearn.manifold import TSNE

from numpy.random import seed
from .firstPreproc import first_prep
from .secondPreprocMicro import second_prep
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import time
from sklearn.manifold import TSNE
from sklearn.ensemble import IsolationForest

seed(1)
def ex(dataframe):
    #df = pd.read_csv('mic.csv')
    #li=first_prep(s3_connection,s3_infos)
    #df=second_prep(li)
    df=dataframe.reset_index()
    coldata=df.columns

    df=df.drop(columns=['mic_error','header'],axis=0)
    df=df.fillna(method="ffill")
    #data_subset=data_subset.drop(['SubjID','trial', 'A2_actiontype'], axis=1)
    a=np.arange(1)
    a=np.tile(a,125)

    time_start = time.time()
    tsne = TSNE(n_components=2, verbose=1, perplexity=40, n_iter=1000)
    tsne_results = tsne.fit_transform(df)

    print('t-SNE done! Time elapsed: {} seconds'.format(time.time()-time_start))

    df2=pd.DataFrame()
    df2['tsne-2d-one'] = tsne_results[:,0]
    df2['tsne-2d-two'] = tsne_results[:,1]
    df2np=df2.to_numpy()
    predClust = IsolationForest(random_state=0,contamination=0.05).fit_predict(df2np)

    predClustdf=pd.DataFrame(predClust)
    predClustdf.to_csv('predclusterdata.csv')


    df_subset=pd.DataFrame()
    df_subset['tsne-2d-one'] = tsne_results[:,0]
    df_subset['tsne-2d-two'] = tsne_results[:,1]
    df_subset['half hour of the day']=predClust

    fig=plt.figure(figsize=(16,10))
    plt.title('5 minutes freq sample')
    sns.scatterplot(
        x="tsne-2d-one", y="tsne-2d-two",
        hue="half hour of the day",
        palette=sns.color_palette("hls", np.size(np.unique(predClust))),
        data=df_subset,
        legend="full",
        alpha=0.8
    )
    plt.legend(loc=2, prop={'size': 6})
    #plt.savefig('mictsne.pdf',bbox_inches='tight')
    return fig


def execute(input_connection_pool,output_connection_pool, *params, session_id):
    global s3_connection, s3_infos, postgres_connection, postgres_infos
    s3_connection=input_connection_pool["s3"]["connObject"]
    s3_infos=input_connection_pool["s3"]["infos"]
    s_id=str(session_id)

    listfile=first_prep(s3_connection, s3_infos)
    dataf=second_prep(listfile)
    f=ex(dataf)

    #salvo il grafico e faccio l'upload su s3
    plt.savefig('mictsne.pdf', bbox_inches='tight')
    stringa="output_file/"+s_id+"_mictsne.pdf"
    s3_connection.upload_file("mictsne.pdf",s3_infos["bucket"],stringa)
    
    #definisco connessione per effettuare la query
    postgres_connection=output_connection_pool["pg-psycopg2"]["connObject"]
    postgres_infos=output_connection_pool["pg-psycopg2"]["infos"]
    tabella=postgres_infos["table"]
    sql="""INSERT INTO {} (sessionid_reference) VALUES (%s)""".format(tabella)
    
    cur=postgres_connection.cursor()

    #inserisco l'id della sessione
    cur.execute(sql,(s_id,))
    
    #inserisco il csv sul db
    with open('predclusterdata.csv','r') as f:
        next(f)
        cur.copy_from(f, tabella , columns=['prediction_cluster_data'])  
    
    
    
    postgres_connection.commit()
    postgres_connection.close()

    print("execution finished, session id: {}".format(session_id))

    return {"retCode": "OK", "description": "Execution terminated with success"}
