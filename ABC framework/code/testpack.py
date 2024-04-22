import sys
#sys.path.append(".")
sys.path.append("m3_abc")
from m3_abc.abcframework import Audit
from m3_abc.abcframework import CollectMetaData
from m3_abc.abcframework import Data_checks


FeedName = "Finance Revenue"
Pipeline_ID="02"
Pipeline_Name="LoadDataX"
Trigger_Name="CopyDataX"
Trigger_ID="02"
Source="Target"
Target="Target"
SourceType=Audit.DB()
TargetType=Audit.DB()
ConnectionSource='Driver={ODBC Driver 17 for SQL Server};Server=tcp:mysqlserver20204.database.windows.net,1433;Database=mysqldb;Uid=mysqluser;Pwd=Mypassword2024;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
ConnectionTarget='Driver={ODBC Driver 17 for SQL Server};Server=tcp:mysqlserver20204.database.windows.net,1433;Database=mysqldb;Uid=mysqluser;Pwd=Mypassword2024;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

OtherParams=([
                {"checks":"Uniqueness", "Source":"Target", "columns":["ID", "RevenueStream"]},
                {"checks":"Volume", "Source":"Target","FilterSource":"ID=0","Target":"Target","FilterTarget":"2=2"},
                {"checks":"Reference", "Source":"Source","Sourcecolumns":["ID", "RevenueStream"],"Target":"Target","Targetcolumns":["ID", "RevenueStream"]}
             ])



audit =  Audit(FeedName,Pipeline_ID, Pipeline_Name,Trigger_Name,Trigger_ID,Source,SourceType,ConnectionSource,Target,TargetType,ConnectionTarget,OtherParams)
STCon = ConnectionTarget
Staging = Target
Sources = None
Type=TargetType
Rule_ID = None



meta=CollectMetaData(Staging,STCon,Type)
Data_checks1 = Data_checks(Pipeline_ID,Sources, Staging,Type,audit,meta,Rule_ID)


