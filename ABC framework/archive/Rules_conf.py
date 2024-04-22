class Rules_conf:
    def __init__(self,Rule_ID,RuleName= None, Valid_From= None,Valid_To= None):
        self.Rule_ID=Rule_ID 
        self.RuleName=RuleName
        self.Valid_From=Valid_From 
        self.Valid_To=Valid_To

    def get_Rule_ID(self):
        return self.Rule_ID

    def set_Rule_ID(self, value):
        self.Rule_ID = value

    def get_RuleName(self):
        return self.RuleName

    def set_RuleName(self, value):
        self.RuleName = value

    def get_Valid_From(self):
        return self.Valid_From

    def set_Valid_From(self, value):
        self.Valid_From = value

    def get_Valid_To(self):
        return self.Valid_To

    def set_Valid_To(self, value):
        self.Valid_To = value
