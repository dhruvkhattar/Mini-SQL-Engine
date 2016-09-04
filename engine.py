#! /usr/bin/env python
#######################################
#     Author : Dhruv Khattar         #
#####################################

import re
import numpy as np
from prettytable import PrettyTable

METAFILE = 'metadata.txt'

class Engine():


    def __init__(self):
        
        self.tables = {}
        self.readMeta()
        self.readTables()
        self.engine()


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
                    self.tables[t.name] = t
                idx += 1


    def readTables(self):

        for table in self.tables:
            with open(table + '.csv') as f:
                for line in f:
                    self.tables[table].n += 1
                    line = line.split(',')
                    idx = 0
                    for col in self.tables[table].attr:
                        self.tables[table].cols[col].append(line[idx].strip())
                        idx += 1


    def processRows(self, query):
        if '*' in query.cols:
            query.cols = []
            for table in query.tables:
                query.cols += self.tables[table].attr

        t = PrettyTable(query.cols)
        for i in self.idx:
            row = []
            for col in query.cols:
                cnt = 0
                for table in query.tables:
                    if col in self.tables[table].cols:
                        cnt += 1
                        row.append(self.tables[table].cols[col][i])
                if cnt > 1:
                    print 'Same Column name in 2 tables.'
            print row
            t.add_row(row)
        print t



    def process(self, query, flag):
        if flag == 1:
            self.idx = xrange(self.tables[query.tables[0]].n)
            self.processRows(query)


    def engine(self):
        
        while True:
            line = raw_input('MiniSQL>')
            queries = line.split(';')
            for q in queries:
                if not q:
                    continue
                query = Query()
                flag = query.parse(line)
                if not flag:
                    continue
                print flag
                print query.cols
                print query.tables
                print query.conds
                self.idx = []
                self.process(query, flag)


class Query():
    
    def __init__(self):
        
        self.tables = []
        self.cols = []
        self.conds = []

    def parse(self, line):

        line = line.strip()
        if not re.match(r'(?i)(^select\ ).+(?i)(\ from\ ).+[;]', line):
            print 'You have an error in your SQL syntax.'
            return 0
        cols = re.sub(r'(?i)(^select\ )(.+)(?i)(\ from\ ).+[;]', r'\2' , line).split(',')
        for col in cols:
            self.cols.append(col.strip())

        if re.search(r'(?i)(\ where\ )', line):
            tables = re.sub(r'(?i)(^select\ ).+(?i)(\ from\ )(.+)(?i)(\ where\ )(.+)[;]', r'\3' , line).split(',')
            for table in tables:
                self.tables.append(table.strip())
            conds = re.sub(r'(?i)(^select\ ).+(?i)(\ from\ ).+(?i)(\ where\ )(.+)[;]', r'\4' , line)
            if re.search(r'(?i)(\ or\ )', conds):
                conds = re.sub(r'^(.+)(?i)(or)(.+)$', r'\1 or \3' , conds).split('or')
                for cond in conds:
                    self.conds.append(cond.strip())
                return 3
            if re.search(r'(?i)(\ and\ )', conds):
                conds = re.sub(r'^(.+)(?i)(and)(.+)$', r'\1 and \3' , conds).split('and')
                for cond in conds:
                    self.conds.append(cond.strip())
                return 4
            self.conds.append(conds.strip())
            return 2
        else:
            tables = re.sub(r'(?i)(^select\ ).+(?i)(\ from\ )(.+)[;]', r'\3' , line).split(',')
            for table in tables:
                self.tables.append(table.strip())
            return 1


class Table():

    def __init__(self):

        self.name = ''
        self.attr = []
        self.cols = {}
        self.n = 0

if __name__ == '__main__':
    sql = Engine()
    sql.engine
