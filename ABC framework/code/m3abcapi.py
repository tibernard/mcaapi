from flask import Flask,request,jsonify
import sys
#sys.path.append(".")
sys.path.append("m3_abc")
from m3_abc.abcframework import Audit, logging
from m3_abc.abcframework import CollectMetaData
from m3_abc.abcframework import Data_checks

# creating a Flask app 
app = Flask(__name__) 
  
# on the terminal type: curl http://127.0.0.1:5000/ 
# returns hello world when we use GET. 


@app.route('/AuditAPI', methods = ['GET', 'POST']) 
def AuditAPI(): 
    if(request.method == 'GET'):
        FeedName  = request.args.get('FeedName', type=str ,default='')
        Pipeline_ID  = request.args.get('change',type=str , default='') 

        Pipeline_Name  = request.args.get('Pipeline_Name', type=str ,default='')
        Trigger_Name  = request.args.get('Trigger_Name',type=str , default='')
        Trigger_ID  = request.args.get('Trigger_ID', type=str ,default='')
        Source  = request.args.get('Source',type=str , default='')
        SourceType  = request.args.get('SourceType', type=str ,default='')
        ConnectionSource  = request.args.get('ConnectionSource',type=str , default='')
        Target  = request.args.get('Target', type=str ,default='')
        TargetType  = request.args.get('TargetType',type=str , default='')
        ConnectionTarget  = request.args.get('ConnectionTarget', type=str ,default='')
        OtherParams  = request.args.get('OtherParams',type=str , default='')
        
        message = ""
        audit =  Audit(FeedName,Pipeline_ID, Pipeline_Name,Trigger_Name,Trigger_ID,Source,SourceType,ConnectionSource,Target,TargetType,ConnectionTarget,OtherParams)
        STCon = ConnectionTarget
        Staging = Target
        Sources = None
        Type=TargetType
        Rule_ID = None
        meta=None
        message += "Audit succeeded!\n"
        try:
            meta=CollectMetaData(Staging,STCon,Type)
            message += "CollectMetaData succeeded!\n"
        except Exception as e:
            print("CollectMetaData->: Something went wrong: Please check values",repr(e))
            message = "CollectMetaData->: Something went wrong. Chceck the logs files"
            logging.exception("Data_checks->Null_Value: Something went wrong: Please check audit paramters")
        Data_checks1 = Data_checks(Pipeline_ID,Sources, Staging,Type,audit,meta,Rule_ID)
        message += "Data checks(Balance) succeeded!\n"
        
        return jsonify({'data': message}) 

# driver function 
if __name__ == '__main__': 
  
    app.run(debug = True)