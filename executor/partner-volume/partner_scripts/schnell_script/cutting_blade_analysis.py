import io
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from scipy import stats


class cuttingBladeAnalysis():
    def __init__(self): #,model_path = '',params_path = ''
        self._model = None
        self._params = None
        #self._model_path = model_path
        #self._params_path = params_path
    
    def loadModel(self,model):
        try:
            self._model=model
            self._model.summary()
            return True
        except Exception as e:
            print(e)
            return False

    def loadParams(self,ndata):
        try:
           
            min_coeffs= np.asarray([float(x) for x in ndata.split('\n')[0].split(':')[-1].split(';') ])
            max_coeffs= np.asarray([float(x) for x in ndata.split('\n')[1].split(':')[-1].split(';') ])
            self._params = {'min_coeffs':min_coeffs, 'max_coeffs':max_coeffs}
            return True
        except Exception as e:
            print(e)
            return False

    def run(self, parsedFile):
        if self._model == None:
            if not self.loadModel(): return  {'error':'failed to load model','message':''} 
        if self._params == None:
            if not self.loadParams(): return  {'error':'failed to load parameters','message':''} 

        try:
            diam = float(getValueFromHeaderVariable(parsedFile,'info_pezzo.diameter'))
            curr = np.asarray(getSignalFromLog(parsedFile,'O_motore_taglio.current_actual_value', 'f3'))    # mask is optional here
            speed = np.asarray(getSignalFromLog(parsedFile,'O_motore_taglio.P_act_velocity', 'f3'))         # mask is optional here
            power = np.abs(np.multiply(curr,speed))
            x = computeFeatures({'motor_current':curr, 'motor_speed':speed,'motor_pw':power,'diameter':diam})
            x = (x - self._params['min_coeffs'])/(self._params['max_coeffs']-self._params['min_coeffs'])
            y = np.argmax(self._model.predict(x))
            cut_class_res = y
            #return {'analysis':[{'type':'cutting_classification','result':int(cut_class_res)}]} #otherwise numpy data are not json serializable
            return {'type':'cutting_classification','result':int(cut_class_res)} #otherwise numpy data are not json serializable
        except Exception as e:
            return {'error':'failed to execute analysis','message':str(e)}


#UTILITIES 

def getMinMaxIdx(v):
    v = np.asarray(v)
    if len(v)<1 : return None
    return [np.argmin(v), np.argmax(v)]

def cleanCurrent(v, minMaxLimit=10,offset=50):
    #return v
    v = np.asarray(v)
    if len(v)<1 : return None
    vd = np.diff(v)
    mM = getMinMaxIdx(vd)
    if (mM[0]-mM[1])<minMaxLimit and len(v)>mM[0]+offset:
        return v[mM[0]+offset:]
    else:
        return v
    
def computeFeatures(sample):
    cc = cleanCurrent(sample['motor_current'])
    ss = sample['motor_speed']
    pw = sample['motor_pw']
    ft = []
    ft.append(sample['diameter'])
    ft.append(np.max(cc))
    ft.append(np.mean(cc))
    ft.append(np.std(cc))
    ft.append(np.trapz(cc)/cc.size)
    ft.append(stats.kurtosis(cc))
    ft.append(stats.skew(cc))
    
    ft.append(np.max(ss))
    ft.append(np.mean(ss))
    ft.append(np.std(ss))
    ft.append(np.trapz(ss)/ss.size)
    ft.append(stats.kurtosis(ss))
    ft.append(stats.skew(ss))
    
    ft.append(np.max(pw))
    ft.append(np.mean(pw))
    ft.append(np.std(pw))
    ft.append(np.trapz(pw)/pw.size)
    ft.append(stats.kurtosis(pw))
    ft.append(stats.skew(pw))
    
    return np.asmatrix(ft)

def getSignalFromLog(parsedFile,targetVariableName, targetMaskName=None):
    dataVarNames = parsedFile['variable_names']
    dataValues = parsedFile['values']
    targetIndex = getIndex(dataVarNames,targetVariableName)
    if targetMaskName!=None:
        targetMaskIndex = getIndex(dataVarNames,targetMaskName)
        f_use_mask = 1
    else:
        f_use_mask = 0
    if not targetIndex: return None
    # if not targetMaskIndex: f_use_mask = 0
    targetIndex = targetIndex[0]
    if f_use_mask: targetMaskIndex = targetMaskIndex[0]
    targetSignal = np.fromiter(map(lambda x : x[targetIndex], dataValues), dtype = float)
    if f_use_mask: targetSignalMask = np.fromiter(map(lambda x : x[targetMaskIndex], dataValues), dtype = float)
    if f_use_mask: targetSignalMasked = targetSignal[targetSignalMask >0]
    #print('Analyzing signal from: %s' % targetVariableName)
    #print('Signal size: %d' % targetSignal.size)
    #if f_use_mask: print('Signal size after masking: %d' % targetSignalMasked.size)
    # SIGNAL ANALYSIS DERIVED FROM MATLAB CODE
    if f_use_mask: targetSignal = targetSignalMasked
    return targetSignal

def getValueFromHeaderVariable(parsedFile, targetVariableName = ''):
    v = next((j for j in parsedFile['headers'] if j["variable_name"] == targetVariableName), None)
    if v:
        return v['value']
    else:
        return None

getIndex = lambda x,xs: [i for (y, i) in zip(x, range(len(x))) if xs == y]
