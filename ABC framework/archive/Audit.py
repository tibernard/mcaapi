
class Audit:
    def __init__(self,Pipeline_ID, Pipeline_Name,Trigger_Name,Trigger_ID,ADF_Path,StorageAccount_ID,Container_ID,Notebook_Path,Start_Date,End_Date):
        self.Pipeline_ID=Pipeline_ID 
        self.Pipeline_Name=Pipeline_Name
        self.Trigger_Name=Trigger_Name
        self.Trigger_ID=Trigger_ID
        self.ADF_Path=ADF_Path
        self.StorageAccount_ID=StorageAccount_ID
        self.Container_ID=Container_ID
        self.Notebook_Path=Notebook_Path
        self.Start_Date=Start_Date
        self.End_Date=End_Date
    
    def set_Rules():
        pass
    def get_Rules():
        pass
     
    def get_Pipeline_ID(self):
        return self.Pipeline_ID

    def set_Pipeline_ID(self, value):
        self.Pipeline_ID = value

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

    def get_ADF_Path(self):
        return self.ADF_Path

    def set_ADF_Path(self, value):
        self.ADF_Path = value

    def get_StorageAccount_ID(self):
        return self.StorageAccount_ID

    def set_StorageAccount_ID(self, value):
        self.StorageAccount_ID = value

    def get_Container_ID(self):
        return self.Container_ID

    def set_Container_ID(self, value):
        self.Container_ID = value

    def get_Notebook_Path(self):
        return self.Notebook_Path

    def set_Notebook_Path(self, value):
        self.Notebook_Path = value

    def get_Start_Date(self):
        return self.Start_Date

    def set_Start_Date(self, value):
        self.Start_Date = value

    def get_End_Date(self):
        return self.End_Date

    def set_End_Date(self, value):
        self.End_Date = value

        
        