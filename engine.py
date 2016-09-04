#! /usr/bin/env python
#######################################
#     Author : Dhruv Khattar         #
#####################################

METAFILE = 'metadata.txt'

class Engine():


    def __init__(self):
        
        self.tables = []
        self.readMeta()
        self.readTables()


    def readMeta(self):
        
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
                        t.cols[data[idx].strip()] = []
                        t.attr.append(data[idx].strip())
                        idx += 1
                    self.tables.append(t)
                idx += 1


    def readTables(self):

        for table in self.tables:
            with open(table.name + '.csv') as f:
                for line in f:
                    line = line.split(',')
                    idx = 0
                    for col in table.cols:
                        table.cols[col].append(line[idx])
                        idx += 1


    def engine(self):
        
        while True:
            line = raw_input('MiniSQL>')
            parse(line)


class Table():

    def __init__(self):

        self.name = ''
        self.attr = []
        self.cols = {}

if __name__ == '__main__':
    sql = Engine()
    sql.engine
