#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 18 08:30:33 2020

@author: luca
"""

import numpy as np
import pandas as pd
import re
from io import BytesIO
import matplotlib.pyplot as plt
import binascii
import os
#re.sub('<.*?>', '', string)
#import bytes

#


def first_prep(connection, connection_info):

#/* Exported structs in dtype ----------------------------------------------------------*/

#define blob
    statistic_t=np.dtype(
        [('avg',np.uint16),
        ('max',np.uint16),
        ('min',np.uint16),
        ('stddev',np.uint16)])


    #accelerometers_fft=np.dtype(
    #    [('acc_x',statistic_t),
    #     ('acc_y',statistic_t),
    #     ('acc_z',statistic_t)])     
        

    blob_t=np.dtype(
        [
        ('header','|S22'),
        
        
        ('label_version','|S6'),
        ('version','|S6'),
        
        #Energy meter
        ('label_energy','|S6'),
        ('energy_error',np.uint8),
        ('active_power_L1',statistic_t),
        ('active_power_L2',statistic_t),
        ('active_power_L3',statistic_t), 
        ('Vrms_L1',statistic_t),
        ('Vrms_L2',statistic_t),
        ('Vrms_L3',statistic_t), 
        ('Irms_L1',statistic_t),
        ('Irms_L2',statistic_t),
        ('Irms_L3',statistic_t),     
        
        #  #Pump
        ('label_pump','|S6'),
        ('pump_error',np.uint8),
        ('preistan',statistic_t),
        ('preinput',statistic_t),
        ('currentp',statistic_t), 
        ('conducil',statistic_t),
        ('temperat',statistic_t),
        ('portatal',statistic_t),     
        ('rotpxmin',statistic_t),
        ('statocfg',statistic_t),
        ('statosys',statistic_t),     
        
        #Steu
        ('label_steu','|S6'),
        ('steu_error',np.uint8),
        ('temperature',(statistic_t,4)),
        ('humidity',(statistic_t,4)),
        
        
        #Micro
        ('label_mic','|S6'),
        ('mic_error',np.uint8),
        ('fft',(statistic_t,256)),
        
        #Accelerometers
        ('label_accs','|S6'),
        ('acc_error',np.uint8),
        ('acc1_x',(statistic_t,256)),
        ('acc1_y',(statistic_t,256)),
        ('acc1_z',(statistic_t,256)),
        ('acc2_x',(statistic_t,256)),
        ('acc2_y',(statistic_t,256)),
        ('acc2_z',(statistic_t,256)),
        ('acc3_x',(statistic_t,256)),
        ('acc3_y',(statistic_t,256)),
        ('acc3_z',(statistic_t,256)),
        ('acc4_x',(statistic_t,256)),
        ('acc4_y',(statistic_t,256)),
        ('acc4_z',(statistic_t,256)),
        ('acc5_x',(statistic_t,256)),
        ('acc5_y',(statistic_t,256)),
        ('acc5_z',(statistic_t,256)),   
        ('hederend','|S2')
        
        ])

    file_name=[]
    file_list=[]

    #vado a prendere la lista di tutti file nel bucket
    for x in connection.list_objects(Bucket=connection_info["bucket"])['Contents']:
        file_name.append(x['Key'])
    
    #apro tutti i file nella lista che si trovano nel percorso data/
    for name in file_name:
        if "data/" in name:
            with BytesIO() as f:
                connection.download_fileobj(Bucket=connection_info["bucket"], Key=name, Fileobj=f)
                f.seek(0)
                q=f.read()
    
    
            an=re.split(b'(?=\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\]0)|(?=\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\][^01])',q)
                
                
            # find header 0 or empty
            aall=re.findall(b'\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\]0|\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\][^01]',q)
            # find header 0 
            a=re.findall(b'\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\]0',q)
                # find header 1
            b=re.findall(b'\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\]1',q)
            # find header empty
            a1=re.findall(b'\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\][^01]',q)
                
                #from header to datetime
            aa = [str(atemp[10:18],'utf-8') for atemp in aall]
            bb = [str(btemp[10:18],'utf-8') for btemp in b]
            aanull = [str(atemp[10:18],'utf-8') for atemp in a1]
                
                
                #importing into dataframe
            aatemp = pd.Series(aa)
            bbtemp=pd.Series(bb)
            aatemp = [e[1:] for e in aatemp]
                
            aapd=pd.DataFrame()
            bbpd=pd.DataFrame()
            aanullpd=pd.DataFrame()
                
                ##aapd datframe header 0 or empty
            aapd['Date']=pd.to_datetime(pd.Series(aa), format='%H:%M:%S')
                ##bbpd datframe header 1
            bbpd['Date']=pd.to_datetime(pd.Series(bb), format='%H:%M:%S')
                ##aanullpd datframe header empty
            aanullpd['Date']=pd.to_datetime(pd.Series(aanull), format='%H:%M:%S')
                
                ##set value for monitoring the flow 0->1->0->1
            aapd['Value']=0
            bbpd['Value']=1
                
                
                #detecting the case of 1->0->0 (second part missing) and 1->1->0 (first part missing)
            mergedf=pd.concat([aapd,bbpd])
            mergedf=mergedf.sort_values(by=['Date'])
            diffsel=mergedf['Value']
            diffa=(diffsel.diff())
            diffabs=diffa.iloc[1:]
            mergedfnew=pd.DataFrame()
            mergedfnew['Date']=mergedf['Date'].iloc[1:]
            mergedfnew['Value']=diffabs
            mergedfnew=mergedfnew.reset_index()
            idsela=mergedfnew.index[mergedfnew.Value==0]
            idsela=idsela.values
            idsela=idsela-1
                
                #take into account eventually consecutive missing packs
            idseladiff=np.diff(idsela)
            idseladiffov=np.where(idseladiff==1)
            idseladiffov=np.asarray(idseladiffov)+1
            idsela[idseladiffov]=idsela[idseladiffov]
                
                #save the info of missing packs in a new dataframe
            idselDate=mergedfnew['Date'].iloc[idsela]
            idselval=mergedfnew['Value'].iloc[idsela]
            datedt=pd.DataFrame()
            datedt['Date']=idselDate
            datedt['Value']=idselval
                #if header of second part replace header of first part
            temp2=datedt.index[datedt.Value==1]
            temp2=temp2.values
            temp2=temp2-1
            datedt=datedt.reset_index()
            brep=datedt.iloc[np.where(datedt.Value==1)]['Date']
            trep=mergedfnew.Date.iloc[temp2]
            for i in range(0,len(brep)):
                datedt=datedt.replace(brep.iloc[i],trep.iloc[i])
                
                #find the index (idsel) of the missing packs in the aapd dataframe (header 0 or empty)
            idsel=[]
            for i in range (0,datedt.shape[0]):
                if np.any(aapd['Date'] == datedt['Date'].iloc[i]):
                    temp=aapd.index[aapd['Date'] == datedt['Date'].iloc[i]]
                    idsel.append(temp.values)           
                    
            idsel=np.asarray(idsel)
            idsel=idsel+1
                
                
                #find the index idsel2 header without 0 and 1 (if not present discard idsel2)
            idsel2=[]
            for i in range (0,aanullpd.shape[0]):
                if np.any(aapd['Date'] == aanullpd['Date'].iloc[i]):
                    temp3=aapd.index[aapd['Date'] == aanullpd['Date'].iloc[i]]
                    idsel2.append(temp3.values)
                
            idsel2=np.asarray(idsel2)
            idsel2=idsel2+1
                
            if idsel2.size != 0:
                idsel=np.concatenate((idsel,idsel2),axis=0)
                idsel=np.unique(idsel)
                idsel=np.sort(idsel)
                
                
                #idscard the total missing packs in the original list
                #save the remaining packs in a list of bytes ansel
            cc=0
            ansel=[]
            count=0
            for ideach in range(0,len(an)):
                if np.all(ideach != idsel): 
                    ansel.append(an[cc])
                    count=count+1
                cc=cc+1
                
                #strech ansel
            if aapd.iloc[0]['Date']>bbpd.iloc[0]['Date']:
                ansel[0]=b'\n'
            anselbin=b''.join(ansel) #.decode('ISO-8859-1')
                
                
                #discard second header
            qnew=re.sub(b'\[\d{2}\/\d{2}\/\d{2}_\d{2}:\d{2}:\d{2}\]1', b'', anselbin)
                

                #qnew=re.sub(b'\n\n', b'', qnew)
                #write the revised bin file
            #fnew=open(name.replace('data/',""),"wb")
            #fnew.write(qnew) 
            #file_list.append(fnew)
            file_list.append(qnew)

    return file_list

                
                 
    
    # #reopen the revised bin file for data visualization
    # fconv = open('incoming/2022/01/17/rev2sgt-F335798FD_648609_CD_01_17.bin','rb')


    # #use the defined blob_t
    # tot = np.fromfile(fconv, dtype=blob_t)

    # #example of data visualization
    # totaccerror=tot['acc_error']
    # erracc=np.unpackbits(totaccerror,axis=0)
    # totaccerror=totaccerror-64
    # mypd=pd.DataFrame({'header':tot['header'],'energy_error':tot['energy_error'],'pump_error':tot['pump_error'],'steu_error':tot['steu_error'],'mic_error':tot['mic_error'],'acc_error':totaccerror})
    # energyactivepower=pd.DataFrame({'acc1_z': tot['acc1_z'][:,128]})
    # energyactivepower=pd.DataFrame(energyactivepower['acc1_z'].tolist(), columns=['avg','max','min','std']) 




    # fig=energyactivepower.plot();
    # plt.xlabel('samples')
    # plt.ylabel('Acc 1z  FFT (mid component)')
    # plt.show()
    # #fig.figure.savefig('/Users/luca/OneDrive - Università Politecnica delle Marche/VRAI_ML/VRAI_Industry40/NuovaSimonelli/code/figure/acc1_z.pdf', format='pdf', dpi=1000, bbox_inches='tight')

