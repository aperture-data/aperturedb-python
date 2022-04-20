class Constraints(object):
    """
    **Interface to specify server side filtering conditions**

    """

    def __init__(self):

        self.constraints = {}

    def equal(self, key, value):

        self.constraints[key] = ["==", value]

    def greaterequal(self, key, value):

        self.constraints[key] = [">=", value]

    def greater(self, key, value):

        self.constraints[key] = [">", value]

    def lessequal(self, key, value):

        self.constraints[key] = ["<=", value]

    def less(self, key, value):

        self.constraints[key] = ["<", value]
