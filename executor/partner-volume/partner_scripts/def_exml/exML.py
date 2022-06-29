import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import cross_val_score, cross_val_predict
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import GridSearchCV
import seaborn as sns
from PyPDF2 import PdfFileMerger
from sklearn.metrics import ConfusionMatrixDisplay
from sklearn.metrics import confusion_matrix
import pickle
from io import BytesIO
from fpdf import FPDF

def write_text(stringa):
    pdf=FPDF()
    pdf.add_page()
    pdf.set_font('Times')
    pdf.cell(0,5,stringa,1,1)
    pdf.output('text.pdf')

def ex():
    
    nomData=str(s3_infos["inputObj"])
    nomi=nomData.split(",")
    #obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=s3_infos["inputObj"])
    #nomData=str(BytesIO(obj['Body'].read()))
    #nomi=nomData.split(",")
    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[0])

    y1=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y1['group']=1


    y1=[y1]

    y1=pd.concat(y1)
    y1=y1.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y1['output'] = 1

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[1])
    y2=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y2['group']=1



    y2=[y2]

    y2=pd.concat(y2)
    y2=y2.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y2['output'] = 2

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[2])
    y3=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y3['group']=1
    y3=y3.drop(['101(Time stamp)','101(Seconds)','Unnamed: 43'],axis=1)
    y3['output'] = 3

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[3])
    y4=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y4['group']=1
    y4=y4.drop(['101(Time stamp)','101(Seconds)','Unnamed: 43'],axis=1)
    y4['output'] = 4

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[4])
    y5=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y5['group']=1
    y5=y5.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y5['output'] = 5

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[5])
    y6=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y6['group']=1



    y6=[y6]
    y6=pd.concat(y6)
    y6=y6.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y6['output'] = 6

    obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[6])
    y7=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y7['group']=1
    y7=y7.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y7['output'] = 7

    obj=s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[7])
    y8=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y8['group']=1

    y8=[y8]
    y8=pd.concat(y8)
    y8=y8.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y8['output'] = 8

    obj=s3_connection.get_object(Bucket=s3_infos["bucket"], Key=nomi[8])
    y9=pd.read_csv(BytesIO(obj['Body'].read()),delimiter=";",decimal=",")
    y9['group']=1




    #y9=pd.concat(y9)
    y9=y9.drop(['101(Time stamp)','101(Seconds)'],axis=1)
    y9['output'] = 9
    # y10=pd.read_csv(r'data/wk06/PROVA 12.csv',delimiter=",",decimal=".")
    # y10=y10.drop(['101(Time stamp)'],axis=1)
    datain=[y1,y2,y3,y4,y5,y6,y7,y8,y9]
    datain=pd.concat(datain)

    corrordabs=np.abs(datain.corr())

    corrordabs.to_csv('corrmatrixabs.csv')


    cbar_kws = dict(extend='max')
    kwargs = dict(linewidths=1, square=True, cbar=True, cbar_kws=cbar_kws)
    plt.figure()
    sns.heatmap(corrordabs,vmin=0.5, vmax=1, **kwargs)
    plt.title('Heat Map - Absolute Pearson Correlation')
    #plt.xlabel('subjects')
    #plt.ylabel('subjects')
    plt.savefig('corrmatrixabs.pdf',bbox_inches='tight',)
    #plt.show()


    Yout=datain['output']
    group=datain['group']
    Xinp=datain.drop(['output','group'],axis=1)

    Xinp=Xinp.fillna(999)
    # Xinp=mydatacodednew.drop(['causa'],axis=1)
    # Yout=mydatacodednew['causa']


    # Xinp=mydatacodednew.drop(['causa'],axis=1)
    # Yout=mydatacodednew['causa']

    inner_cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=5)
    outer_cv = StratifiedKFold(n_splits=10, shuffle=True, random_state=5)

    model_DT = DecisionTreeClassifier(random_state=1)
    p_grid_dt = {"max_depth": [1, 5, 10, 20]}


    clf_dt = GridSearchCV(estimator=model_DT, param_grid=p_grid_dt, cv=inner_cv)

    scores_DT = cross_val_score (clf_dt, X=Xinp, y=Yout, cv=outer_cv, scoring='accuracy')
    accDT = scores_DT.mean()
    #print(accDT)
    predictions_DT = cross_val_predict (clf_dt, X=Xinp, y=Yout, cv=outer_cv)

    CM_DT = confusion_matrix (Yout,predictions_DT, labels=[1,2,3,4,5,6,7,8,9])
    CM_DT_display = ConfusionMatrixDisplay(CM_DT, display_labels=['B-SA','B-SC', 'PE-SA', 'PE-SC','OLA','IE','CGC','CGO','CGOp']).plot(cmap='Purples').ax_.set(xlabel='DT Predictions Classificazione Classificazione Condizioni 1-9', ylabel='True')
    clf_dt.fit (Xinp,Yout)

    best_model = clf_dt.best_estimator_

    importance_tot=best_model.feature_importances_
    predictors=Xinp.columns

    indices = np.argsort(importance_tot.flatten())[::-1]
    #indices = indices.ravel()
    indices = indices[:10]
    names = [predictors[i] for i in indices]
    importance_tot = np.transpose(importance_tot)
    importance_tot = importance_tot.ravel()
    plot1=plt.figure()
    plt.barh(range(indices.shape[0]), importance_tot[indices])
    plt.yticks(range(indices.shape[0]), names)
    plt.xlabel('Features Importance Decision Tree: Classificazione Condizioni 1-9')
    #plt.show()
    plot1.savefig('featimp.pdf')



    filename = 'finalized_model.sav'
    pickle.dump(best_model, open(filename, 'wb'))
    
    # some time later...
    
    # load the model from disk
    loaded_model = pickle.load(open(filename, 'rb'))

    #test on the same dataset 
    result = loaded_model.score(Xinp, Yout)
    #print(result)

    stringa = "accuracy dt: "+ str(accDT) + "  best model results: " + str(result)

    write_text(stringa)

    paths=['corrmatrixabs.pdf', 'featimp.pdf', 'text.pdf']

    merger=PdfFileMerger()
    for path in paths:
        merger.append(path)
    merger.write(local_file_connection + "report.pdf") #qua aggiungere local_file_connection + "report.pdf"
    merger.close()

def execute(input_connection_pool, output_connection_pool, *params, session_id):
    global s3_connection, s3_infos, local_file_connection
    s3_connection = input_connection_pool["s3"]["connObject"]
    s3_infos = input_connection_pool["s3"]["infos"]

    
    local_file_connection = output_connection_pool["local_file"]["connObject"]
    ex()

    print("execution finished, session id: {}".format(session_id))

    return {"retCode": "OK", "description": "Execution terminated with success"}
