import copy
import math
import json
import array
import csv
from datetime import datetime
import logging
import os
from venv import logger
import pyodbc, struct
import json
import time
from io import StringIO

#sys.path.append("m3_abc")


strreturn = "\n"

Currentdir = os.path.dirname(os.path.realpath(__file__))
directory =Currentdir+os.sep+"errors_logs"+os.sep
v_filename = "abc_errors"+str(datetime.now())[0:10].replace("-","").replace(":","").replace(" ","")
#logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')

logging.basicConfig(filename=directory+v_filename + '.log', filemode='a', level=logging.DEBUG)


class Audit:
   
     
    def __init__(self,FeedName,Pipeline_ID,Pipeline_Name,Trigger_Name,Trigger_ID,Source,SourceType,ConnectionSource,Target,TargetType,ConnectionTarget,OtherParams=None):
        self.FeedName = FeedName
        self.Pipeline_ID=Pipeline_ID 
        self.Pipeline_Name=Pipeline_Name
        self.Trigger_Name=Trigger_Name
        self.Trigger_ID=Trigger_ID
        self.Trigger_Startdate = datetime.now()
        self.Trigger_Enddate = time.time()
        self.Source=Source
        self.Target=Target
        self.SourceType=SourceType
        self.TargetType=TargetType
        self.ConnectionSource=ConnectionSource
        self.ConnectionTarget=ConnectionTarget
        self.OtherParams=OtherParams
# Static Params




    def DB():
        return "db"
    
    def CONTAINER():
        return "dl"
    
    def DELTALAKE():
        return "dl"
     # End Static Params
    
    def set_Rules():
        pass
    def get_Rules():
        pass

    def get_FeedName(self):
        return self.FeedName
    
    def get_Pipeline_ID(self):
        return self.Pipeline_ID


    def set_FeedName(self, value):
        self.FeedName = value

    def set_Pipeline_ID(self, value):
        self.Pipeline_ID = value

    def get_Trigger_Startdate(self):
     return str(self.Trigger_Startdate) #str(self.Trigger_Startdate)
    
    def set_Trigger_Startdate(self, value):
        self.Trigger_Startdate = value

    def get_Trigger_Enddate(self):
        return self.Trigger_Enddate

    def set_Trigger_Enddate(self, value):
        self.Trigger_Enddate = value

    def get_Pipeline_Name(self):
        return self.Pipeline_Name
        

    def set_Pipeline_Name(self, value):
        self.Pipeline_Name = value

    def get_Trigger_Name(self):
        return self.Trigger_Name

    def set_Trigger_Name(self, value):
        self.Trigger_Name = value

    def get_Trigger_ID(self):
        return self.Trigger_ID

    def set_Trigger_ID(self, value):
        self.Trigger_ID = value

    def get_Source(self):
        return self.Source

    def set_Source(self, value):
        self.Source = value

    def get_Target(self):
        return self.Target

    def set_Target(self, value):
        self.Target = value

    def get_SourceType(self):
        return self.SourceType

    def set_SourceType(self, value):
        self.SourceType = value

    def get_TargetType(self):
        return self.TargetType

    def set_TargetType(self, value):
        self.TargetType = value

    def get_ConnectionSource(self):
        return self.ConnectionSource

    def set_ConnectionSource(self, value):
        self.ConnectionSource = value

    def get_ConnectionTarget(self):
        return self.ConnectionTarget

    def set_ConnectionTarget(self, value):
        self.ConnectionTarget = value

    def get_OtherParams(self):
        return self.OtherParams

    def set_OtherParams(self, value):
        self.OtherParams = value

    
    
     
    

        
        



class CollectMetaData:
    
    def __init__(self,Staging, STCon,Type=None):
        # Check the nature of the staging (db->for database;sa-> storage account gen2,dl-> for delta lake)
        self.STCon = STCon
        self.Staging = Staging
        self.Type= Type


        match self.Type:
            case "db":
                #self.getsqlserverMeta()
                pass
            case "sa":
                pass
            case "dl":
                pass
            case _:
                 raise Exception("CollectMetaData:SourceType paramter Is Missing.")
    def get_STCon(self):
        return self.STCon

    def set_STCon(self, value):
        self.STCon = value

    def get_Staging(self):
        return self.Staging

    def set_Staging(self, value):
        self.Staging = value

    def get_Type(self):
        return self.Type

    def set_Type(self, value):
        self.Type = value
    def add_customMetrics(SourceMetrics,Targetmerics,QuerySource,QueryTarget,EvalOperator,PassThreshold):
        pass

    def getsqlserverMeta(self):
        cnxn = pyodbc.connect(self.STCon)
        QueryStr="""SELECT 
                        """+"""'"""+self.Type+"'"+""" 'Object Type',
                        """+"""'"""+self.Staging+"'"+""" 'Object Name',
                        c.name 'Column Name',
                        t.Name 'Data type',
                        c.max_length 'Max Length',
                        c.precision ,
                        c.scale ,
                        c.is_nullable,
                        ISNULL(i.is_primary_key, 0) 'Primary Key'
                        FROM    
                        sys.columns c
                        INNER JOIN 
                        sys.types t ON c.user_type_id = t.user_type_id
                        LEFT OUTER JOIN 
                        sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        LEFT OUTER JOIN 
                        sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        WHERE
                        c.object_id = OBJECT_ID('"""+self.Staging+"""')"""
        names = ['Object Type','Object Name','Column Name', 'Data type', 'Max Length','precision','scale','is_nullable','Primary Key']
        cursor = cnxn.cursor()	
        cursor.execute(QueryStr) 
        row = cursor.fetchone() 
        rown = 0
    
        MetaDic = dict(zip(names, row))
        Tuples = list()
        while row:
            Tuples.append(dict(zip(names, row)))
            row = cursor.fetchone()
            rown+=1

        return Tuples
        #for l in Tuples:
            ###print(str(l["Object Type"] )+ "  " + str(l["Object Name"] )+ "  "+str(l["Column Name"]) +"  "+ str(l["Data type"])  )


class Data_checks(CollectMetaData,Audit):
    def __init__(self,Pipeline_ID,Sources, Target,Types,Audit,Meta,Rule_ID):
        #super().__init__(Rule_ID)
        self.Pipeline_ID=Pipeline_ID 
        self.Sources=Sources
        self.Targets=Target 
        self.Types=Types
        self.Audit=Audit 
        self.Meta=Meta
        self.Rule_ID = Rule_ID
        
        try:
            try:
                print("Before")
                self.Null_Value_Checks()
                print("After")
            except Exception as e:
                print("Data_checks->Null_Value: Something went wrong: Please check OtherParams values",repr(e))
                logging.exception("Data_checks->Null_Value: Something went wrong: Please check OtherParams values")
                
                
 
            Tuples=self.Audit.get_OtherParams()
            for l in Tuples:

                
                    match l["checks"]:
                        case "Uniqueness":
                            try:
                                self.Meta.set_Staging(l["Source"])
                                self.Uniqueness_Checks(l["Source"],l["columns"])
                            except Exception as e:
                                #print("Data_checks->Uniqueness: Something went wrong: Please check OtherParams values",repr(e))
                                logging.exception("Data_checks->Uniqueness: Something went wrong: Please check OtherParams values")
                                #logging.error("Data_checks->Uniqueness: Something went wrong: Please check OtherParams values",repr(e))
                        case "Volume":
                            try:
                                Source = l["Source"]
                                FilterSource = l["FilterSource"]
                                Target = l["Target"]
                                FilterTarget = l["FilterTarget"]
                                if (bool(FilterSource=="None") != bool(FilterTarget=="None")):
                                    raise Exception("In case filters are provided both must not None")
                                elif (FilterSource !="None"):
                                    self.Volume_Checks(Source,Target,FilterSource,FilterTarget)
                                else:
                                    self.Volume_Checks(Source,Target)
                            except Exception as e:
                                logging.exception("Data_checks->Volume: Something went wrong: Please check OtherParams values")
                                #print("Data_checks->Volume: Something went wrong: Please check OtherParams values",repr(e))
                            
                        case "Reference":
                            try:
                                Source = l["Source"]
                                Sourcecolumns = l["Sourcecolumns"]
                                Target = l["Target"]
                                Targetcolumns = l["Targetcolumns"]
                                self.Referencial_Checks(Source,Target,Sourcecolumns,Targetcolumns)
                            except Exception as e:
                                logging.exception("Data_checks->Reference: Something went wrong: Please check OtherParams values")
                                print("Data_checks->Reference: Something went wrong: Please check OtherParams values",repr(e))    
                        case _:
                            pass
                    
                
        except Exception as e:
            print("Data_checks: Something went wrong: Please check OtherParams values",repr(e))
        finally:
            print("Data_checks: The 'try except' on OtherParams is finished")  
                
                
                
                

            


# TBD

    def Null_Value_Checks(self):
        match self.Meta.get_Type():
            case "db":

                cnxn = pyodbc.connect(self.Meta.get_STCon())
                ###print(self.Meta.get_STCon())
                Tuples = self.Meta.getsqlserverMeta()
                names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Target','Column_Name','NULLS_CNT']
                columnsaffected=0
                Null_Value_Checks_List=list()
                for l in Tuples:
                    #get_Trigger_Startdate
                    StrQuery= """SELECT 
                        """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                        """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                        """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                        """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                        """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                        """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                        """+"""'"""+self.Meta.get_Staging()+"'"+""" Target,
                        """+"""'"""+str(l["Column Name"])+"'"+""" Column_Name,
                        COUNT(1) NULLS_CNT
                        FROM
                        """+""" """+self.Meta.get_Staging()+""" 
                        WHERE
                        """+""" """+str(l["Column Name"])+"""  IS NULL"""#cursor = cnxn.cursor()	
                    
                    cursor = cnxn.cursor()
                    cursor.execute(StrQuery) 
                    row = cursor.fetchone() 

                    names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Target','Column_Name','NULLS_CNT']
                    
                
                    MetaDic = dict(zip(names, row))
                    Tuples = list()
                    while row:
                        Tuples.append(dict(zip(names, row)))
                        row = cursor.fetchone()
                    
                    for l in Tuples:
                        if int(l["NULLS_CNT"])==0:
                          tx = list()
                          tx.clear()
                          tx.append (str(l["FeedName"] )+ "," + str(l["Pipeline_ID"] )+ ","+str(l["Pipeline_Name"]) +","+ str(l["Trigger_ID"]) +","+ str(l["Trigger_Name"]) +","+str(l["Trigger_Startdate"]) +","+ str(l["Target"]) +","+ str(l["Column_Name"]) +","+ str(l["NULLS_CNT"]) )
                    Null_Value_Checks_List.append(tx)
                    columnsaffected =+1
                    
                if (columnsaffected>0):
                    # Specify the directory path
                    ##print(Null_Value_Checks_List)
                    Currentdir = os.path.dirname(os.path.realpath(__file__))
                    directory =Currentdir+os.sep+"logs"+os.sep+str(l["FeedName"] )+os.sep+str(l["Pipeline_Name"])+os.sep+"Null_Value_Checks"+os.sep+str(datetime.now())[0:10].replace("-","")
                    v_filename = "nullvaluechecks"+str(datetime.now())[0:19].replace("-","").replace(":","").replace(" ","")

                    #import os
                    if not os.path.exists(directory):
                        os.makedirs(directory)
                    start_time = time.time()
                        
                    
                    start_time = time.time()
                    with open(directory+os.sep+v_filename+".txt", 'w') as f:
                            writer = csv.writer(f , lineterminator='\n')
                            writer.writerow(names)
                            writer.writerows(Null_Value_Checks_List)
                    print("Null_Value_Checks_List--- %s seconds ---" % (time.time() - start_time))
                        
                    
                    
                pass
            case "sa":
                pass
            case "dl":
                pass
            case _:
                raise Exception("Datachecks->Null_Value_Checks:Source Type Is Missing.")
        
    def Uniqueness_Checks(self,location,columns):
        match self.Meta.get_Type():
            case "db":
                cnxn = pyodbc.connect(self.Meta.get_STCon())
                ###print(self.Meta.get_STCon())
                Tuples = self.Meta.getsqlserverMeta()
                names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Target','Column_Name','NULLS_CNT']
                Uniqueness_Checks_List=list()
                Uniqueness_Checks_List.clear()
                columnsaffected=0
                for l in columns:
                   
                    StrQuery= """SELECT 
                        """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                        """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                        """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                        """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                        """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                        """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                        """+"""'"""+self.Meta.get_Staging()+"'"+""" Target,
                        """+"""'"""+str(l)+"'"+""" Column_Name,
                        """+""""""+str(l)+""+""" Column_Value,
                        COUNT(1) DUPLICATE_CNT
                        FROM
                        """+""" """+self.Meta.get_Staging()+""" 
                        GROUP BY
                        """+""" """+str(l)+"""  
                         HAVING COUNT(1)>1"""
                    
                    cursor = cnxn.cursor()
                    cursor.execute(StrQuery) 
                    row = cursor.fetchone() 
                    names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Target','Column_Name','Column_Value','DUPLICATE_CNT']

                    
                    MetaDic = dict(zip(names, row))
                    Tuples = list()
                    Tuples.clear()
                    while row:
                        Tuples.append(dict(zip(names, row)))
                        row = cursor.fetchone()
                    ###print("Before:",Uniqueness_Checks_List)
                    columnsaffected = 0
                    for l in Tuples:
                        if int(l["DUPLICATE_CNT"])>1:
                          tx = list()
                          tx.clear()
                          tx.append (str(l["FeedName"] )+ "," + str(l["Pipeline_ID"] )+ ","+str(l["Pipeline_Name"]) +","+ str(l["Trigger_ID"]) +","+ str(l["Trigger_Name"]) +","+str(l["Trigger_Startdate"]) +","+ str(l["Target"]) +","+ str(l["Column_Name"]) +","+ str(l["Column_Value"]) +","+ str(l["DUPLICATE_CNT"]) )
                          Uniqueness_Checks_List.append(tx)
                          columnsaffected =columnsaffected+1
                    
                    if (columnsaffected>0):
                        # Specify the directory path
                        Currentdir = os.path.dirname(os.path.realpath(__file__))
                        directory =Currentdir+os.sep+"logs"+os.sep+str(l["FeedName"] )+os.sep+str(l["Pipeline_Name"])+os.sep+"Uniqueness_Check"+os.sep+str(datetime.now())[0:10].replace("-","")
                        v_filename = "uniquenesschecks"+str(datetime.now())[0:19].replace("-","").replace(":","").replace(" ","")
                        ##print(Uniqueness_Checks_List)
                        #import os
                        if not os.path.exists(directory):
                            os.makedirs(directory)
                        start_time = time.time()
                        with open(directory+os.sep+v_filename+".txt", 'w') as f:
                            writer = csv.writer(f , lineterminator='\n')
                            writer.writerow(names)
                            writer.writerows(Uniqueness_Checks_List)
                        print("Uniqueness_Checks_List--- %s seconds ---" % (time.time() - start_time))

        
    def Volume_Checks(self, Source,Target,FilterSource=None,FilterTarget=None):
        
        def convertToNumber (s):
            return int.from_bytes(s.encode(), 'little')

        def convertFromNumber (n):
            return n.to_bytes(math.ceil(n.bit_length() / 8), 'little').decode()
        if(FilterSource!=None):
            match self.Meta.get_Type():
                case "db":
                    cnxn = pyodbc.connect(self.Meta.get_STCon())
                    ###print(self.Meta.get_STCon())
                    Tuples = self.Meta.getsqlserverMeta()
                    names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Target','Column_Name','NULLS_CNT']
                    Volume_Checks_List=list()
                    Volume_Checks_List.clear()
                    columnsaffected=0
                    FilterSource
                    StrQuery= """WITH CT1 AS (SELECT 
                        """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                        """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                        """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                        """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                        """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                        """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                        """+"""'"""+Source+"'"+""" Source,
                        COUNT(1) ROW_CNT
                        FROM
                        """+""" """+Source+""" 
                        WHERE 1 = 1 AND ("""+FilterSource+""" ))"""
                    StrQuery+= """, CT2 AS (SELECT 
                        """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                        """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                        """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                        """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                        """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                        """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                        """+"""'"""+Target+"'"+""" Target,
                        COUNT(1) ROW_CNT
                        FROM
                        """+""" """+Target+""" 
                        WHERE 1 = 1 AND ("""+FilterTarget+""" )
                        ) SELECT CT1.FeedName,CT1.Pipeline_ID,CT1.Pipeline_Name,CT1.Trigger_ID,
                            CT1.Trigger_Name,CT1.Trigger_Startdate,CT1.Source,
                            CT2.Target,CT1.ROW_CNT as SOURCE_ROW_CNT,
                            CT2.ROW_CNT as TARGET_ROW_CNT, 
                            CT1.ROW_CNT- CT2.ROW_CNT AS ROWS_CNT_DIFF
                            FROM CT1,CT2""" 
                    
                    ##print(StrQuery)
                    cursor = cnxn.cursor()
                    cursor.execute(StrQuery) 
                    row = cursor.fetchone() 
                    names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Source','Target','SOURCE_ROW_CNT','TARGET_ROW_CNT','ROWS_CNT_DIFF']

                    
                    MetaDic = dict(zip(names, row))
                    Tuples = list()
                    Tuples.clear()
                    while row:
                        Tuples.append(dict(zip(names, row)))
                        row = cursor.fetchone()
                    ##print("Before:",Volume_Checks_List)
                    columnsaffected = 0
                    for l in Tuples:
                        if int(l["ROWS_CNT_DIFF"])!=0:
                            tx = list()
                            tx.clear()
                            tx.append (str(l["FeedName"] )+ "," + str(l["Pipeline_ID"] )+ ","+str(l["Pipeline_Name"]) +","+ str(l["Trigger_ID"]) +","+ str(l["Trigger_Name"]) +","+str(l["Trigger_Startdate"]) +","+ str(l["Source"]) +","+ str(l["Target"]) +","+ str(l["SOURCE_ROW_CNT"]) +","+ str(l["TARGET_ROW_CNT"]) )
                            Volume_Checks_List.append(tx)
                            columnsaffected =columnsaffected+1
                    
                    if (columnsaffected>0):
                        # Specify the directory path
                        Currentdir = os.path.dirname(os.path.realpath(__file__))
                        directory =Currentdir+os.sep+"logs"+os.sep+str(l["FeedName"] )+os.sep+str(l["Pipeline_Name"])+os.sep+"Volume_Check"+os.sep+str(datetime.now())[0:10].replace("-","")
                        v_filename = "volumechecks"+str(datetime.now())[0:19].replace("-","").replace(":","").replace(" ","")
                        v_Trendfilename = Source+Target+"Trend"
                        
                        if not os.path.exists(directory):
                            os.makedirs(directory)
                        start_time = time.time()
                        names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Source','Target','SOURCE_ROW_CNT','TARGET_ROW_CNT']
                        with open(directory+os.sep+v_filename+".txt", 'w') as f:
                            writer = csv.writer(f , lineterminator='\n')
                            writer.writerow(names)
                            writer.writerows(Volume_Checks_List)

                        with open(directory+os.sep+v_Trendfilename+".txt", 'a') as f:
                            writer = csv.writer(f , lineterminator='\n')
                            writer.writerows(Volume_Checks_List)
                        print("Volume_Checks_List--- %s seconds ---" % (time.time() - start_time))
        else:
            pass
    def Referencial_Checks(self, Source,Target,Sourcecolumns,Targetcolumns):
        def Quotes(a):
            b = '"' + a + '"'
            return b
        match self.Meta.get_Type():
            case "db":
                cnxn = pyodbc.connect(self.Meta.get_STCon())
                
                Referencial_Checks_List=list()
                Referencial_Checks_List.clear()
                columnsaffected=0
                Prv=""
                Prv2=""
                ExistsFilter=""
                CT1AdditionalColumns=""
                CT2AdditionalColumns=""
                DynamicNames=""
                DynamicRetieval=""
                names = ['FeedName','Pipeline_ID','Pipeline_Name', 'Trigger_ID', 'Trigger_Name','Trigger_Startdate','Source']
                for l1 in Sourcecolumns:
                    for l2 in Targetcolumns:
                        if (l1==Prv or l2==Prv2):
                         continue
                        Prv=copy.copy(l1)
                        Prv2=copy.copy(l2) 
                        ExistsFilter +=" AND CT1."+str(l1)+"=CT2."+str(l2)
                        CT1AdditionalColumns+=", "+str(l1)
                        CT2AdditionalColumns+=", "+str(l2)
                        DynamicNames += ", '"+str(l1)+"'"
                        DynamicRetieval += ","+ """str(l["""+Quotes(str(l1))+"""])"""
                        names.append(l1)
                        

                StrQuery= """WITH CT1 AS (SELECT 
                        """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                        """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                        """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                        """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                        """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                        """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                        """+"""'"""+Source+"'"+""" Source
                        """+""""""+CT1AdditionalColumns+""+""" 
                        FROM
                        """+""" """+Source+""" 
                       )"""
                StrQuery+= """, CT2 AS (SELECT 
                    """+"""'"""+self.Audit.get_FeedName()+"'"+""" FeedName,
                    """+"""'"""+self.Audit.get_Pipeline_ID()+"'"+""" Pipeline_ID,
                    """+"""'"""+self.Audit.get_Pipeline_Name()+"'"+""" Pipeline_Name,
                    """+"""'"""+self.Audit.get_Trigger_ID()+"'"+""" Trigger_ID,
                    """+"""'"""+self.Audit.get_Trigger_Name()+"'"+""" Trigger_Name,
                    """+"""'"""+self.Audit.get_Trigger_Startdate()+"'"+""" Trigger_Startdate,
                    """+"""'"""+Target+"'"+""" Target
                    """+""""""+CT2AdditionalColumns+""+""" 
                    FROM
                    """+""" """+Target+""" 

                    ) SELECT CT1.*
                        FROM CT1 WHERE NOT EXISTS(SELECT 1 from CT2 WHERE 1=1 """+ExistsFilter+""")"""
                
                ##print(StrQuery)
                cursor = cnxn.cursor()
                cursor.execute(StrQuery) 
                row = cursor.fetchone() 
        
                MetaDic = dict(zip(names, row))
                ##print(MetaDic)
                Tuples = list()
                Tuples.clear()
                while row:
                    Tuples.append(dict(zip(names, row)))
                    row = cursor.fetchone()

                columnsaffected = 0
                str11="""str(l["FeedName"] ), str(l["Pipeline_ID"] ),str(l["Pipeline_Name"]), str(l["Trigger_ID"]) , str(l["Trigger_Name"]) ,str(l["Trigger_Startdate"]) ,str(l["Source"])"""""
                for l in Tuples:
                    tx = list()
                    tx.clear()
                    
                    for l1 in DynamicRetieval:
                        str11+=l1
                    #DynamicRetievalMM = """str(l["""+Quotes(str('ID'))+"""])"""+","+"""str(l["""+Quotes(str('RevenueStream'))+"""])"""
                    #print(str(l["FeedName"] )+ "," + str(l["Pipeline_ID"] )+ ","+str(l["Pipeline_Name"]) +","+ str(l["Trigger_ID"]) +","+ str(l["Trigger_Name"]) +","+str(l["Trigger_Startdate"]) +","+ str(l["Source"]) +eval(str(DynamicRetievalMM)))
                    tx.append (eval(str11))
                    Referencial_Checks_List.append(tx)
                    columnsaffected =columnsaffected+1
  
        if (columnsaffected>0):
                            # Specify the directory path
                            Currentdir = os.path.dirname(os.path.realpath(__file__))
                            directory =Currentdir+os.sep+"logs"+os.sep+str(l["FeedName"] )+os.sep+str(l["Pipeline_Name"])+os.sep+"Referencial_Check"+os.sep+str(datetime.now())[0:10].replace("-","")
                            v_filename = "referencialchecks"+str(datetime.now())[0:19].replace("-","").replace(":","").replace(" ","")
                            
                            if not os.path.exists(directory):
                                os.makedirs(directory)
                            start_time = time.time()
                            with open(directory+os.sep+v_filename+".txt", 'w') as f:
                                writer = csv.writer(f , lineterminator='\n')
                                writer.writerow(names)
                                writer.writerows(Referencial_Checks_List)
                            print("Referencial_Checks_List--- %s seconds ---" % (time.time() - start_time))

                
                
                
                
                       
                   
                    
    def String_Patterns_Checks(self, value):
        pass
    def Freshness_Checks(self, value):
        pass
    def Custom_Metrics_Checks(self, value):
        pass
# End TBD    


    def get_Pipeline_ID(self):
        return self.Pipeline_ID

    def set_Pipeline_ID(self, value):
        self.Pipeline_ID = value

    def get_Sources(self):
        return self.Sources

    def set_Sources(self, value):
        self.Sources = value

    def get_Targets(self):
        return self.Targets

    def set_Targets(self, value):
        self.Targets = value

    def get_Types(self):
        return self.Types

    def set_Types(self, value):
        self.Types = value

    def get_Meta(self):
        return self.Meta

    def set_Meta(self, value):
        self.Meta = value

    def get_Rule_ID(self):
        return self.Rule_ID

    def set_Rule_ID(self, value):
        self.Rule_ID = value





