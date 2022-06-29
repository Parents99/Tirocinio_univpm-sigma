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
import glob
from numpy.lib.recfunctions import structured_to_unstructured
#re.sub('<.*?>', '', string)
#import bytes

#

def second_prep(newfile_list):

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

    #open file
    totheaderdate=[]
    count=0
    #ist_of_files = sorted(filter( os.path.isfile,
    #                        glob.glob('sgt' + '*') ))
    
    
    totaccerror=[]
    totacc1x=[]
    totacc1y=[]
    totacc1z=[]
    list_of_files=[]

    #for i in newfile_list:
    #    list_of_files.append(i.name)
   

    #newfile_list.sort(key=lambda x: getattr(x,'name'))
    
    #for files in list_of_files:
    #     print(files)
    #    fconv = open(files,'rb')
        #print(files)
    for fconv in newfile_list:
    # #use the defined blob_t
        
        #tot=(np.fromfile(fconv, dtype=blob_t))
        tot=(np.frombuffer(fconv, dtype=blob_t))
        uu=np.where(tot['acc_error']==1)
        uu=np.asarray(uu)
        uu=uu.ravel()
        if np.size(uu)!=0:
            if uu[0]==1:
                continue
            else:
                tot=tot[:uu[0]-1]
        
            
        totacc1x.append((tot['fft']))
        #totacc1y.append((tot['acc1_y']))
        #totacc1z.append(tot['acc1_z'])
        totaccerror.append((tot['mic_error']))
        tempheader = [str(atemp[2:19],'utf-8') for atemp in tot['header']]
        totheaderdate.append(pd.to_datetime(pd.Series(tempheader), format='%y/%m/%d_%H:%M:%S',errors='coerce'))
        count=count+1
        


    dfheader=pd.concat([totheaderdate[l] for l in range(0,count)],axis=0)
    dfaccerror=np.concatenate([(totaccerror[l]) for l in range(0,count)],axis=0)

    dfacc1x=np.concatenate([(totacc1x[l]) for l in range(0,count)],axis=0)
    # dfacc1y=np.concatenate([(totacc1y[l]) for l in range(0,count)],axis=0)
    # dfacc1z=np.concatenate([(totacc1z[l]) for l in range(0,count)],axis=0)

    dfacc1x=structured_to_unstructured(dfacc1x)
    # dfacc1y=structured_to_unstructured(dfacc1y)
    # dfacc1z=structured_to_unstructured(dfacc1z)

    # dfacc1x=dfacc1x.transpose(1,2,0)
    # dfacc1y=dfacc1y.transpose(1,2,0)
    # dfacc1z=dfacc1z.transpose(1,2,0)

    dfacc1x=dfacc1x.reshape(dfacc1x.shape[0],dfacc1x.shape[1]*dfacc1x.shape[2])
    # dfacc1y=dfacc1y.reshape(dfacc1y.shape[0],dfacc1y.shape[1]*dfacc1y.shape[2])
    # dfacc1z=dfacc1z.reshape(dfacc1z.shape[0],dfacc1z.shape[1]*dfacc1z.shape[2])

    dfnp=dfacc1x



    df=pd.DataFrame(data=dfnp,index=range(0,dfnp.shape[0]),columns=range(0,dfnp.shape[1]))
    df.insert(0, 'mic_error', dfaccerror)
    df.insert(0, 'header', dfheader.reset_index(drop=True))

    df.fillna(method="ffill")
    df.index=(df['header'])
    #df=df.drop(['header'],axis=1)

    dfup=df.groupby(pd.Grouper(freq='5T')).mean()

    #dfup.to_csv('mic.csv')
    #df = pd.concat([dfheader, pd.DataFrame(dfaccerror)], axis=1)
    return dfup

        
    #totheader=pd.DataFrame(totheader)
    # aa = [str(atemp[10:18],'utf-8') for atemp in aall]
    # bb = [str(btemp[10:18],'utf-8') for btemp in b]
    # aanull = [str(atemp[10:18],'utf-8') for atemp in a1]
                
                
    #             #importing into dataframe
    # aatemp = pd.Series(aa)
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

