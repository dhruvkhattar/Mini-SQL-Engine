#! /usr/bin/env python
#######################################
#     Author : Dhruv Khattar         #
#####################################

from __future__ import division
import re
import numpy as np
import sys
import itertools
from prettytable import PrettyTable

METAFILE = 'metadata.txt'

class Engine():


    def __init__(self):
        
        self.tables = {}
        self.tn = {}
        self.tabs = []
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
                    self.tabs.append(t.name)
                idx += 1


    def readTables(self):

        for table in self.tables:
            with open(table + '.csv') as f:
                for line in f:
                    self.tables[table].n += 1
                    line = line.split(',')
                    idx = 0
                    for col in self.tables[table].attr:
                        line[idx] = int(line[idx].strip())
                        self.tables[table].cols[col].append(line[idx])
                        idx += 1
                    self.tables[table].rows.append(line)

    def processCols(self, query):

        fcols = []
        if '*' in query.cols:
            query.cols = []
            for table in query.tables:
                fcols += [ table + '.' + x for x in self.tables[table].attr]
        else:
            for col in query.cols:
                if re.match(r'.+\..+', col):
                    fcols.append(col)
                else:
                    cnt = 0
                    for table in query.tables:
                        if col in self.tables[table].cols:
                            tab = table
                            fcols.append(tab + '.' + col)
                            cnt += 1
                    if cnt > 1:
                        print 'Same Column name in 2 tables.'
                        return -1
                    if cnt == 0:
                        print 'Column not found'
                        return -1
        return fcols

    def processRows(self, query):
        
        fcols = self.processCols(query)
        if fcols == -1:
            return 

        t = PrettyTable(fcols)
        for i in self.idx:
            row = []
            for col in fcols:
                tab = re.sub(r'(.+)\.(.+)', r'\1', col)
                co = re.sub(r'(.+)\.(.+)', r'\2', col)
                if tab not in self.tables:
                    print 'Table ' + tab + ' not found.'
                    return
                if co not in self.tables[tab].attr:
                    print 'Column ' + co + ' not found.'
                    return
                
                cn = 0
                for j in self.tables[tab].attr:
                    if j == co:
                        break;
                    cn += 1
                row.append(self.outtable[i][self.tn[tab]][cn])
            t.add_row(row)
        print t


    def processAgg(self, query):
        
        t = PrettyTable(query.cols)
        row = []
        for col in query.cols:
            func = re.sub(r'\(.+\)', '', col)
            attr = re.sub(r'.+\((.+)\)', r'\1', col).strip()
            tab = ''
            if re.match(r'.+\..+', attr):
                tab = re.sub(r'(.+)\.(.+)', r'\1', attr)
                co = re.sub(r'(.+)\.(.+)', r'\2', attr)
            else:
                cnt = 0
                co = attr
                for table in query.tables:
                    if attr in self.tables[table].cols:
                        tab = table
                        cnt += 1
                if cnt > 1:
                    print 'Same Column name in 2 tables.'
                    return
            if tab not in self.tables:
                print 'Table ' + tab + ' not found.'
                return
            if co not in self.tables[tab].attr:
                print 'Column ' + co + ' not found.'
                return
            cn = 0
            for j in self.tables[tab].attr:
                if j == co:
                    break;
                cn += 1

            if re.match(r'(?i)(sum)',func):
                agg = 0
                for i in self.idx:
                    agg += self.outtable[i][self.tn[tab]][cn]
            elif re.match(r'(?i)(max)',func):
                agg = -sys.maxint - 1
                for i in self.idx:
                    agg = max(agg, self.outtable[i][self.tn[tab]][cn])
            elif re.match(r'(?i)(min)',func):
                agg = sys.maxint
                for i in self.idx:
                    agg = min(agg, self.outtable[i][self.tn[tab]][cn])
            elif re.match(r'(?i)(avg)',func):
                agg = 0
                for i in self.idx:
                    agg += self.outtable[i][self.tn[tab]][cn]
                agg /= len(self.idx) 
            
            row.append(agg)
        t.add_row(row)
        print t


    def processDistinct(self, query):
        
        if len(query.cols) > 1:
            print 'Distinct can only be used with one column'
            return
        t = PrettyTable(query.cols)        
        col = re.sub(r'.+\((.+)\)', r'\1', query.cols[0]).strip()
        tab = ''
        if re.match(r'.+\..+', col):
            tab = re.sub(r'(.+)\.(.+)', r'\1', col)
            co = re.sub(r'(.+)\.(.+)', r'\2', col)
        else:
            cnt = 0
            co = col
            for table in query.tables:
                if col in self.tables[table].cols:
                    tab = table
                    cnt += 1
            if cnt > 1:
                print 'Same Column name in 2 tables.'
                return
        if tab not in self.tables:
            print 'Table ' + tab + ' not found.'
            return
        if co not in self.tables[tab].attr:
            print 'Column ' + co + ' not found.'
            return
        distinct = {}
        for i in self.idx:
            row = []
            cn = 0
            for j in self.tables[tab].attr:
                if j == co:
                    break;
                cn += 1
            if self.outtable[i][self.tn[tab]][cn] not in distinct:
                row.append(self.outtable[i][self.tn[tab]][cn])
                distinct[self.outtable[i][self.tn[tab]][cn]] = 1
            if row:
                t.add_row(row)
        print t


    def process(self, query, flag):
        
        if flag == 1:
            n = 1;
            for i in query.tables:
                n *= self.tables[i].n
            self.idx = xrange(n)
        elif flag == 2:
            ret = self.processCondition(query, 0)
            if ret == -1:
                return
            else:
                self.idx = ret
        else:
            print flag
            ret1 = self.processCondition(query, 0)
            ret2 = self.processCondition(query, 1)
            if ret1 == -1 or ret2 == -1:
                return
            elif flag == 3:
                self.idx = list(set(ret1) | set(ret2))
            elif flag == 4:
                self.idx = list(set(ret1) & set(ret2))
        if any(re.match(r'(?i)(distinct)', word) for word in query.cols):
            self.processDistinct(query)
            return
        for word in query.cols:
            if re.match(r'.+\(.+\)', word):
                self.processAgg(query)
                return
        self.processRows(query)

    def processCondition(self, query, idx):

        ind = []
        if not re.match(r'([^<>=]+)(<|=|>|<>|<=|>=)([^<>=]+)', query.conds[idx]):
            print 'Invalid Conditon'
            return -1
        
        lhs = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)', r'\1', query.conds[idx]).strip()
        op = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)', r'\2', query.conds[idx]).strip()
        rhs = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)', r'\3', query.conds[idx]).strip()

        val = 0
        try:
            rhs = int(rhs)
            val = 1
        except ValueError:
            val = 0
       
        tab = ''
        if re.match(r'(.+)\.(.+)', lhs):
            tab = re.sub(r'(.+)\.(.+)', r'\1', lhs)
            co = re.sub(r'(.+)\.(.+)', r'\2', lhs)
        else:
            co = lhs
            for table in query.tables:
                cnt = 0
                if co in self.tables[table].cols:
                    tab = table
                    cnt += 1
            if cnt > 1:
                print 'Same Column name in 2 tables.'
                return -1
        
        if tab not in self.tables:
            print 'Table ' + tab + ' not found.'
            return -1
        if co not in self.tables[tab].attr:
            print 'Column ' + co + ' not found.'
            return -1

        if val:
            idx = 0
            cn = 0
            for j in self.tables[tab].attr:
                if j == co:
                    break;
                cn += 1
            n = 1;
            for i in query.tables:
                n *= self.tables[i].n
            for i in xrange(n):
                if self.check(self.outtable[i][self.tn[tab]][cn], op, rhs):
                    ind.append(idx)
                idx += 1
        else:
            rtable = re.sub(r'(.+)\.(.+)', r'\1', rhs)
            rcol = re.sub(r'(.+)\.(.+)', r'\2', rhs)
        return ind

    def check(self, lhs, op, rhs):

        if op == '=':
            return lhs == rhs
        elif op == '>':
            return lhs > rhs
        elif op == '<':
            return lhs < rhs
        elif op == '>=':
            return lhs >= rhs
        elif op == '<=':
            return lhs <= rhs
        elif op == '<>':
            return lhs != rhs

    
    def retrieveTables(self, table):

        return self.tables[table].rows
    
    def retrieveCols(self, table):

        return self.tables[table].attr

    def engine(self):
        
        while True:
            line = raw_input('MiniSQL>')
            if line == 'exit':
                break
            queries = line.split(';')
            for q in queries:
                self.outtable = []
                self.outcols = []
                if not q:
                    continue
                query = Query()
                flag = query.parse(line)
                if not flag:
                    continue
                cnt = 0
                fg = 0
                for i in query.tables:
                    if i not in self.tables:
                        print 'Table not found.'
                        fg = 1
                        break
                    self.tn[i] = cnt
                    cnt += 1

                if fg:
                    continue

                for i in itertools.product(*map(self.retrieveTables,query.tables)):
                    self.outtable.append(i)
                self.outcols = map(self.retrieveCols, query.tables)
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
        self.rows = []
        self.n = 0

if __name__ == '__main__':
    sql = Engine()
    sql.engine
