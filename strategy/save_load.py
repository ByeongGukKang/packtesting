import json
from copy import deepcopy

def _var_replacer(string, vars=[]):
    """Variable replacer.

    Args:
        string (string): a string line of code
        vars (list): variables to replace, list with string elements.
    
    Returns:
        string in vars will be replaced as self.string
    """
    already_self = ["self."+var for var in vars]
    for var in vars:
        if var not in already_self:
            string = string.replace(var, "self."+var)
    for var in vars:
        if "read_csv" in string:
            if "self." + var in string.split("read_csv")[-1]:
                string = string[:string.find("read_csv")] + string[string.find("read_csv"):].replace("self."+var, var)
    return string

def save(current, export, variables=[], file_type="py", encode="utf-8", see_variables=False):
    """Save the Strategy.
    
    Args:
        current (str): name of current file
        export (str): name of strategy file you want to export
        variables (list)(optional): list of variables you want to remain in the class as self. ,
                                    if not given the formatter changes the variables automatically 
        file_type ("py","ipynb"): expandsion of exported file, "py" is recommended
                                    only with "py" file, you can load the strategy as an object
        endoce (): choose the encoding type
        see_variables (bool): if True, it shows which variables are changed as self.
    
    Returns:
        This creats a new file
    """
    if ".ipynb" not in current:
        current = current+".ipynb"

    variables = set(variables)
    isVarAuto = False
    if len(variables) == 0:
        isVarAuto = True
    
    with open(current, 'r', encoding = encode) as f:
        ipython_cells = json.load(f)
    
    if file_type == "py":
        code_cells = []
        for cell in ipython_cells["cells"]:
            code_cells.append(cell["source"])
        
        final_return = []
        final_package = []
        final_return.append("\n\n\nclass StratObj:\n")
        for cell in code_cells:
            totalSave = False
            isPackage = False
            isData = False
            for line in range(len(cell)):
                if "#$$$<package>" in cell[line]:
                    isPackage = True

                if (isPackage == False) & (isVarAuto == True):
                    if "#$$$<data>" in cell[line]:
                        final_return.append("\n"+" "*4 + "def data(self):\n")
                        isData = True
                    elif "#$$$<preprocess>" in cell[line]:
                        final_return.append("\n"+" "*4 + "def preprocess(self):\n")
                    elif "#$$$<score>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def score(self):\n")
                    elif "#$$$<backtest>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def backtest(self):\n")

                    if "#$$$" in cell[line]:
                        if totalSave == False:
                            totalSave = True
                        elif totalSave == True:
                            totalSave = False
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))
                    elif totalSave == True:
                        tmp_var = cell[line].replace("==","")
                        if "=" in tmp_var:
                            tmp_var = tmp_var.split("=")[0].replace(" ","")
                            if "." not in tmp_var:
                                variables.add(tmp_var)
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))
                    elif "#$" in cell[line]:
                        tmp_var = cell[line].replace("==","")
                        if "=" in tmp_var:
                            tmp_var = tmp_var.split("=")[0].replace(" ","")
                            if "." not in tmp_var:
                                variables.add(tmp_var)
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))

                elif isPackage == False:
                    if "#$$$<data>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def data(self):\n")
                    elif "#$$$<preprocess>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def preprocess(self):\n")
                    elif "#$$$<score>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def score(self):\n")
                    elif "#$$$<backtest>" in cell[line]:
                        final_return.append("\n")
                        final_return.append("\n"+" "*4 + "def backtest(self):\n")

                    if "#$$$" in cell[line]:
                        if totalSave == False:
                            totalSave = True
                        elif totalSave == True:
                            totalSave = False
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))
                    elif totalSave == True:
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))
                    elif "#$" in cell[line]:
                        final_return.append(" "*8 + _var_replacer(cell[line], variables))

                else:
                    if "#$$$" in cell[line]:
                        if totalSave == False:
                            totalSave = True
                        elif totalSave == True:
                            totalSave = False
                        final_package.append(cell[line])
                    elif totalSave == True:
                        final_package.append(cell[line])
                    elif "#$" in cell[line]:
                        final_package.append(cell[line])
        
        for i in range(len(final_package)):
            final_return.insert(i, final_package[i])

        export = export + "." + file_type
        with open(export, 'w', encoding=encode) as f:
            for line in final_return:
                f.write(line)

        if see_variables == True:
            print(variables)

    elif file_type == "ipynb":
        cell_length = range(len(ipython_cells["cells"]))

        for cell in cell_length:
            tmp_cell = deepcopy(ipython_cells["cells"][cell])

            ipython_cells["cells"][cell]["execution_count"] = None
            totalSave = False

            line_length = range(len(ipython_cells["cells"][cell]["source"]))
            for line in line_length:
                if "#$$$" in ipython_cells["cells"][cell]["source"][line]:
                    if totalSave == False:
                        totalSave = True
                    elif totalSave == True:
                        totalSave = False
                    pass
                elif totalSave == True:
                    pass
                elif "#$" in ipython_cells["cells"][cell]["source"][line]:
                    pass
                else:
                    tmp = ipython_cells["cells"][cell]["source"][line]
                    tmp_cell["source"].remove(tmp)
                
            if len(tmp_cell['source']) != len(ipython_cells["cells"][cell]["source"]):
                ipython_cells["cells"][cell] = tmp_cell

        ipython_cells["cells"] = [cell for cell in ipython_cells["cells"] if len(cell["source"]) != 0]
                
        export = export + "." + file_type
        with open(export, 'w', encoding=encode) as f:
            json.dump(ipython_cells, f)


def load(file):
    """Load the strategy.

    Args:
        file (str): file name of strategy saved as .py file

    Returns:
        An strategy object 
    """
    if ".py" in file:
        file = file.replace(".py","")
    codes = "from "+file+" import StratObj"
    exec(codes, globals())
    result = StratObj()
    return result