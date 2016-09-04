#! /usr/bin/env python
#######################################
#     Author : Dhruv Khattar         #
#####################################

METAFILE = 'metadata.txt'

class Engine():
    
    def __init__(self):
        
        self.tables = []
        self.parseMeta()
    
    def parseMeta(self):
        
        with open(METAFILE) as f:
            data = f.readlines()
            idx = 0
            while idx < len(data):
                if data[idx].strip() == '<begin_table>':
                    t = Table()
                    idx += 1
                    t.name = data[idx].strip()
                    idx += 1
                    while data[idx].strip() != '<end_table>':
                        t.cols.append(data[idx].strip())
                        idx += 1
                    self.tables.append(t)
                idx += 1

    def engine(self):
        
        while True:
            line = raw_input('MiniSQL>')
            parse(line)


class Table():

    def __init__(self):

        self.name = ''
        self.cols = []

if __name__ == '__main__':
    sql = Engine()
    sql.engine

