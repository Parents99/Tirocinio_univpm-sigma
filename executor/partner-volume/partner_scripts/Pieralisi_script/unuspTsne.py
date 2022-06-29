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
from fcntl import F_SEAL_SHRINK

from sqlalchemy import column


from .preproc import pre_process

import pandas as pd
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import manhattan_distances
from sklearn.manifold import TSNE
from sklearn.ensemble import IsolationForest


from sklearn.metrics.cluster import adjusted_rand_score
from numpy.random import seed

import numpy as np
from sklearn import cluster
import matplotlib.pyplot as plt
import seaborn as sns
import time

seed(1)

def ex(connection, info):
    #df = pd.read_csv('input.csv')
    df=pre_process(connection, info)
    coldata=df.columns
    df=df.reset_index('TimesTamp')
    #print(newd.head(5))
    #data_subset=data_subset.drop(['SubjID','trial', 'A2_actiontype'], axis=1)
    # a=np.arange(0,48)
    # a=np.tile(a,31)
    df['TimesTamp'] = pd.to_datetime(df['TimesTamp'])

    # a=df['TimesTamp']-df['TimesTamp'].iloc[0]
    # a=a.dt.days
    a=np.zeros((df.shape[0],1))

    df=df.drop(columns=['TimesTamp'],axis=0)


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
    predClustdf.to_csv('predclusterdata.csv', index=False)


    df_subset=pd.DataFrame()
    df_subset['tsne-2d-one'] = tsne_results[:,0]
    df_subset['tsne-2d-two'] = tsne_results[:,1]
    df_subset['half hour of the day']=predClust


    numpal=np.size(np.unique(a))
    fig=plt.figure(figsize=(16,10))
    sns.scatterplot(
        x="tsne-2d-one", y="tsne-2d-two",
        hue="half hour of the day",
        palette=sns.color_palette("hls", np.size(np.unique(predClust))),
        data=df_subset,
        legend="full",
        alpha=0.8
    )
    plt.legend(loc=2, prop={'size': 10})
    #plt.savefig('tsne.pdf',bbox_inches='tight')
    return fig

def execute(input_connection_pool, output_connection_pool, *params, session_id):
    global s3_connection, s3_infos,postgres_connection, postgres_infos
    s3_connection=input_connection_pool["s3"]["connObject"]
    s3_infos=input_connection_pool["s3"]["infos"]
    s_id=str(session_id)

    fig=ex(s3_connection,s3_infos)
    plt.savefig('tsne.pdf', bbox_inches='tight')
    stringa="output_file/"+s_id+"_tsne.pdf"
    
    s3_connection.upload_file("tsne.pdf",s3_infos["bucket"],stringa)

    postgres_connection=output_connection_pool["pg-psycopg2"]["connObject"]
    postgres_infos=output_connection_pool["pg-psycopg2"]["infos"]
    
    tabella=postgres_infos["table"]
    sql="""INSERT INTO {} (sessionid_reference) VALUES (%s)""".format(tabella)

    cur=postgres_connection.cursor()
    cur.execute(sql,(s_id,))
    
    with open('predclusterdata.csv','r') as f:
        next(f)
        cur.copy_from(f, tabella, sep=',', columns=['prediction_clustered_data'])  #out_pieralisi e prediction_cluster_data
    
    postgres_connection.commit()
    postgres_connection.close()




    return {"retCode": "OK", "description": "Execution terminated with success"}