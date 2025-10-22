import matplotlib.pyplot as plt

class Histogram():
      def show(title, xlabel, ylabel, labels, values):
            if(not Histogram._check_show_parameters(title, xlabel, ylabel, labels, values)):
                return 0
        
            plt.bar(labels, values)

            plt.title(title)
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)

            plt.show()
        
      
      def _check_show_parameters(title, xlabel, ylabel, labels, values):
            if(title == None or title == ""):
              print("Title is not valid")
              return False
            if(xlabel == None or xlabel == ""):
              print("xlabel is not valid")
              return False
            if(ylabel == None or ylabel == ""):
              print("ylabel is not valid")
              return False
            if(labels == None or len(labels) == 0):
              print("Labels are empty")
              return False
            if(values == None or len(values) == 0):
              print("Values are empty")
              return False
            return True