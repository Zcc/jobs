class Job:
    def __init__(self, name, parameter):
        self.name = name
        self.parameter = parameter

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
