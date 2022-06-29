#ALGORITMO TAGLIO 1
from glob import glob
from .cutting_blade_analysis import cuttingBladeAnalysis
from io import BytesIO
from tensorflow.keras.models import load_model
import tensorflow as tf
import h5py

def parseLogFile(fContent):
   fParts = fContent.split('#\n')
   fHeader = fParts[0]
   fData = fParts[1]
   # PARSING HEADER
   headerRows = fHeader.split('\n')
   headerData = []
   for j in range(len(headerRows)):
      r = headerRows[j].split(';')
      if len(r)>2: headerData.append({'name': r[0], 'variable_name': r[1], 'value': r[2]})
   
   #PARSING DATA
   dataRows = fData.split('\n')
   dataNames = dataRows[0].split(';')
   dataVarNames = dataRows[1].split(';')
   dataValues = []
   for j in range(2,len(dataRows)-3):
      dataValues.append([ float(x) for x in dataRows[j].split(';') ])
   
   #DATA AS OBJECT
   #data = []
   #for j in range(len(dataNames)):
   #   data.append({'name': dataNames[j], 'variable_name': dataVarNames[j], 'values': np.fromiter(map(lambda x : x[j], dataValues),dtype = float)})
   
   return {'names': dataNames, 'variable_names': dataVarNames, 'values': dataValues, 'headers': headerData}

#_cut_model = cuttingBladeAnalysis('models\\taglio_03_2020\\model.h5','models\\taglio_03_2020\\normalization.txt')


def example():
   _cut_model = cuttingBladeAnalysis()
   with BytesIO() as file:
      s3_connection.download_fileobj(Bucket=s3_infos["bucket"], Key=s3_infos["model_path"], Fileobj=file)
      file.seek(0)
      h=h5py.File(file,'r')
      mod=tf.keras.models.load_model(h)
   
   with BytesIO() as f:
      s3_connection.download_fileobj(Bucket=s3_infos["bucket"], Key=s3_infos["add_file"], Fileobj=f)
      f.seek(0)
      ndat=f.read()
         
   n=str(ndat,encoding="utf-8")
   ndata=n.replace("'","").replace("b","").replace('\r','')

   _cut_model.loadModel(model=mod)
   _cut_model.loadParams(ndata=ndata)
  
   obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=s3_infos["input_path"]) 
   con1=obj['Body'].read()
   content1= con1.decode("utf-8").replace('\r','')
   
   log1=parseLogFile(content1)

   obj = s3_connection.get_object(Bucket=s3_infos["bucket"], Key=s3_infos["input_path2"])
   con1=obj['Body'].read()
   content2= con1.decode("utf-8").replace('\r','')

   log2=parseLogFile(content2)

   res1=_cut_model.run(log1)
   
   res2 = _cut_model.run(log2)

   values1=[res1["type"],res1["result"],"taglio_03_2020","sample_log_ok.csv"]
   values2=[res2["type"],res2["result"],"taglio_03_2020","sample_log_ko.csv"]
   return values1,values2
   



def execute(input_connection_pool, output_connection_pool, *params, session_id):
   global s3_connection, s3_infos, local_file_connection,  postgres_connection, postgres_infos
   s3_connection=input_connection_pool["s3"]["connObject"]
   s3_infos=input_connection_pool["s3"]["infos"]
   
   values=example()
   values1=list(values[0])
   values2=list(values[1])

   postgres_connection = output_connection_pool["pg-psycopg2"]["connObject"]
   postgres_infos=output_connection_pool["pg-psycopg2"]["infos"]

   tabella=postgres_infos["table"]

   cur=postgres_connection.cursor()
   
   sql = """ INSERT INTO {} (type, result, model_reference, dataset_reference) VALUES (%s,%s,%s,%s) """.format(tabella)
   cur.execute(sql,values1) 
   cur.execute(sql,values2)
   postgres_connection.commit()
   postgres_connection.close()
    
   print("execution finished, session id: {}".format(session_id))

   return {"retCode": "OK", "description": "Execution terminated with success"}