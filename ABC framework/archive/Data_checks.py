import sys
sys.path.append("m3_abc")
#import Rules_conf
import CollectMetadata

class Data_checks():
    def __init__(self,Pipeline_ID,Sources, Target,Types,Meta,Rule_ID):
        #super().__init__(Rule_ID)
        self.Pipeline_ID=Pipeline_ID 
        self.Sources=Sources
        self.Targets=Target 
        self.Types=Types 
        self.Meta=Meta
        self.Rule_ID = Rule_ID
        print(type(self.Meta))


# TBD
    def Null_Value_Checks(self, value):
        pass
    def Uniqueness_Checks(self, value):
        pass
    def Volume_Checks(self, value):
        pass
    def Referencial_Checks(self, value):
        pass
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
