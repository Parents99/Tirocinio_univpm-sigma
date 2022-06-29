#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 18 11:16:14 2022

@author: luca
"""

import pandas as pd
from io import BytesIO

def pre_process(conn,inf):
    s3_c=conn
    s3_i=inf

    #pulley_vert_vel=pd.read_csv('data/01_pulley_vert_vel.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/01_pulley_vert_vel.csv")
    pulley_vert_vel=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)

    pulley_vert_vel['TimesTamp']=pd.to_datetime(pulley_vert_vel['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    pulley_vert_vel.index=(pulley_vert_vel['TimesTamp'])

    pulley_vert_velup=pulley_vert_vel.groupby(pd.Grouper(freq='60T')).mean()
    del pulley_vert_vel



    #pulley_vert_cuscinetto=pd.read_csv('data/02_pulley_vert_cuscinetto.csv',sep=';',decimal=',',
    #   encoding='latin-1')

    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/02_pulley_vert_cuscinetto.csv")
    pulley_vert_cuscinetto=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)

    pulley_vert_cuscinetto['TimesTamp']=pd.to_datetime(pulley_vert_cuscinetto['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    pulley_vert_cuscinetto.index=(pulley_vert_cuscinetto['TimesTamp'])

    pulley_vert_cuscinettoup=pulley_vert_cuscinetto.groupby(pd.Grouper(freq='60T')).mean()
    del pulley_vert_cuscinetto



    #pulley_vert_temp=pd.read_csv('data/03_pulley_vert_temp.csv',sep=';',decimal=',',
    #    encoding='latin-1')

    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/03_pulley_vert_temp.csv")
    pulley_vert_temp=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    pulley_vert_temp['TimesTamp']=pd.to_datetime(pulley_vert_temp['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    pulley_vert_temp.index=(pulley_vert_temp['TimesTamp'])

    pulley_vert_tempup=pulley_vert_temp.groupby(pd.Grouper(freq='60T')).mean()
    del pulley_vert_temp



    #pulley_horiz_vel=pd.read_csv('data/04_pulley_horiz_vel.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/04_pulley_horiz_vel.csv")
    pulley_horiz_vel=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    pulley_horiz_vel['TimesTamp']=pd.to_datetime(pulley_horiz_vel['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    pulley_horiz_vel.index=(pulley_horiz_vel['TimesTamp'])

    pulley_horiz_velup=pulley_horiz_vel.groupby(pd.Grouper(freq='60T')).mean()
    del pulley_horiz_vel


    #pulley_horiz_cuscinetto=pd.read_csv('data/05_pulley_horiz_cuscinetto.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/05_pulley_horiz_cuscinetto.csv")
    pulley_horiz_cuscinetto=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    pulley_horiz_cuscinetto['TimesTamp']=pd.to_datetime(pulley_horiz_cuscinetto['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    pulley_horiz_cuscinetto.index=(pulley_horiz_cuscinetto['TimesTamp'])

    pulley_horiz_cuscinettoup=pulley_horiz_cuscinetto.groupby(pd.Grouper(freq='60T')).mean()
    del pulley_horiz_cuscinetto



    #collector_vert_vel=pd.read_csv('data/06_collector_vert_vel.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/06_collector_vert_vel.csv")
    collector_vert_vel=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_vert_vel['TimesTamp']=pd.to_datetime(collector_vert_vel['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_vert_vel.index=(collector_vert_vel['TimesTamp'])

    collector_vert_velup=collector_vert_vel.groupby(pd.Grouper(freq='60T')).mean()
    del collector_vert_vel



    #collector_vert_cuscinetto=pd.read_csv('data/07_collector_vert_cuscinetto.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/07_collector_vert_cuscinetto.csv")
    collector_vert_cuscinetto=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_vert_cuscinetto['TimesTamp']=pd.to_datetime(collector_vert_cuscinetto['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_vert_cuscinetto.index=(collector_vert_cuscinetto['TimesTamp'])

    collector_vert_cuscinettoup=collector_vert_cuscinetto.groupby(pd.Grouper(freq='60T')).mean()
    del collector_vert_cuscinetto



    #collector_vert_temperatura=pd.read_csv('data/08_collector_vert_temperatura.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/08_collector_vert_temperatura.csv")
    collector_vert_temperatura=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_vert_temperatura['TimesTamp']=pd.to_datetime(collector_vert_temperatura['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_vert_temperatura.index=(collector_vert_temperatura['TimesTamp'])

    collector_vert_temperaturaup=collector_vert_temperatura.groupby(pd.Grouper(freq='60T')).mean()
    del collector_vert_temperatura


    #collector_horiz_vel=pd.read_csv('data/09_collector_horiz_vel.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/09_collector_horiz_vel.csv")
    collector_horiz_vel=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_horiz_vel['TimesTamp']=pd.to_datetime(collector_horiz_vel['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_horiz_vel.index=(collector_horiz_vel['TimesTamp'])

    collector_horiz_velup=collector_horiz_vel.groupby(pd.Grouper(freq='60T')).mean()
    del collector_horiz_vel


    #collector_horiz_cuscinetto=pd.read_csv('data/10_collector_horiz_cuscinetto.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/10_collector_horiz_cuscinetto.csv")
    collector_horiz_cuscinetto=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_horiz_cuscinetto['TimesTamp']=pd.to_datetime(collector_horiz_cuscinetto['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_horiz_cuscinetto.index=(collector_horiz_cuscinetto['TimesTamp'])

    collector_horiz_cuscinettoup=collector_horiz_cuscinetto.groupby(pd.Grouper(freq='60T')).mean()
    del collector_horiz_cuscinetto



    #collector_long_vel=pd.read_csv('data/11_collector_long_vel.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/11_collector_long_vel.csv")
    collector_long_vel=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    collector_long_vel['TimesTamp']=pd.to_datetime(collector_long_vel['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    collector_long_vel.index=(collector_long_vel['TimesTamp'])

    collector_long_velup=collector_long_vel.groupby(pd.Grouper(freq='60T')).mean()
    del collector_long_vel


    #Temp_PT100_pulley=pd.read_csv('data/12_Temp_PT100_pulley.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/12_Temp_PT100_pulley.csv")
    Temp_PT100_pulley=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    Temp_PT100_pulley['TimesTamp']=pd.to_datetime(Temp_PT100_pulley['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    Temp_PT100_pulley.index=(Temp_PT100_pulley['TimesTamp'])

    Temp_PT100_pulleyup=Temp_PT100_pulley.groupby(pd.Grouper(freq='60T')).mean()
    del Temp_PT100_pulley


    #Temp_PT100_collector=pd.read_csv('data/13_Temp_PT100_collector.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/13_Temp_PT100_collector.csv")
    Temp_PT100_collector=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    Temp_PT100_collector['TimesTamp']=pd.to_datetime(Temp_PT100_collector['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    Temp_PT100_collector.index=(Temp_PT100_collector['TimesTamp'])

    Temp_PT100_collectorup=Temp_PT100_collector.groupby(pd.Grouper(freq='60T')).mean()
    del Temp_PT100_collector


    #speed_rotor=pd.read_csv('data/14_speed_rotor.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/14_speed_rotor.csv")
    speed_rotor=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    speed_rotor['TimesTamp']=pd.to_datetime(speed_rotor['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    speed_rotor.index=(speed_rotor['TimesTamp'])

    speed_rotorup=speed_rotor.groupby(pd.Grouper(freq='60T')).mean()
    del speed_rotor


    #speed_tamb=pd.read_csv('data/15_speed_tamb.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/15_speed_tamb.csv")
    speed_tamb=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    speed_tamb['TimesTamp']=pd.to_datetime(speed_tamb['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    speed_tamb.index=(speed_tamb['TimesTamp'])

    speed_tambup=speed_tamb.groupby(pd.Grouper(freq='60T')).mean()
    del speed_tamb


    #speed_stator=pd.read_csv('data/16_speed_stator.csv',sep=';',decimal=',',
    #    encoding='latin-1')
    obj = s3_c.get_object(Bucket=s3_i["bucket"], Key="data/16_speed_stator.csv")
    speed_stator=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",", encoding='latin-1', skipinitialspace=True)
    speed_stator['TimesTamp']=pd.to_datetime(speed_stator['TimesTamp'], format='%d/%m/%Y %H:%M:%S')
    speed_stator.index=(speed_stator['TimesTamp'])

    speed_statorup=speed_stator.groupby(pd.Grouper(freq='60T')).mean()
    del speed_stator

    data = pd.concat([speed_statorup, speed_tambup, speed_rotorup, Temp_PT100_collectorup, Temp_PT100_pulleyup, 
                    collector_long_velup, collector_horiz_cuscinettoup, collector_horiz_velup, collector_vert_temperaturaup,
                    collector_vert_cuscinettoup, collector_vert_velup, pulley_horiz_cuscinettoup, pulley_horiz_velup,
                    pulley_vert_tempup,pulley_vert_cuscinettoup,pulley_vert_velup],axis=1,  join="outer")
                    
    data=data.fillna(method='ffill')               
    data=data.fillna(method='bfill')               
                    
    return data             