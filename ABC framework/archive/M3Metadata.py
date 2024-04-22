import array
import os
import pyodbc, struct

class M3Metadata:
    
    def __init__(self,Staging, STCon,Type=None):
        # Check the nature of the staging (db->for database;sa-> storage account gen2,dl-> for delta lake)
        self.STCon = STCon
        self.Staging = Staging
        self.Type= Type
        match Type:
            case "db":
                self.getsqlserverMeta()
                pass
            case "sa":
                pass
            case "dl":
                pass
            case _:
                pass

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
        
        for l in Tuples:
            print(str(l["Object Type"] )+ "  " + str(l["Object Name"] )+ "  "+str(l["Column Name"]) +"  "+ str(l["Data type"])  )
        
                
             
            
        

