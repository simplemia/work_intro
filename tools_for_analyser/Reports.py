#/usr/bin/python
#-*- coding: utf-8 -*-
#author=mia

import sys
sys.path.append('/yjcom/libs/python')
import copy
import re
import Utils
import Tools 
from optparse import OptionParser  
import Extract


E = 1

def map_tags(extract,inputs):
    L_date = []
    for input in inputs:
        date = input.split('/')[-1]
        args = ['-k',r"lambda x:\"\t\".join(x)+\"\t%s\""%date]
        extract.run('MAP','',input,*args)
        L_date.append(date)
    return L_date

def merge_hdfs(extract,inputs,L_key):
    args = ['-r','%s'%(','.join(L_key))]
    extract.run('MERGE',DATE,inputs,*args)

def __merge_hdfs(extract,map_inputs):
    date = ''
    map_inputs = extract.OUTDIRS
    args = ['-r','%s'%(','.join(L_date))]
    extract.run('MERGE',date,map_inputs,*args)

def merge_local(ins):
    extract = subExtract()
    extract.run('COUNT',date,dir,*args)
    ins = []
    for ind in extract.OUTDIRS:
        ins = extract.download_hdfs(ind)
        ins.append(ins)
    L_date,D_coll = [],{}
    for input in ins:
        date = input.split('.')[-1]
        L_date.append(date)
        for i in open(input):
            count,key = i.strip().split()
            if key in D_coll:
                D_coll[key].append(count)
            else:
                D_coll[key] = []
                D_coll[key].append(count)
    out_csv = open('report/%s.merge'%ins[0].split('.')[0],'w')
    out_csv.write('merge,'+','.join(L_date))
    for name,count in D_coll.items():
        out_csv.write(name+','+','.join(count))
    out_csv.close()

def main(dir,*args):
    extract = Extract.Extract()
    extract.EXECUTE = E
    extract.DL = 0
    print DATE
    extract.run('EXTRACT',DATE,dir,*args)
    ex_outdirs = copy.copy(extract.OUTDIRS)
    extract.OUTDIRS = []
    L_key = map_tags(extract,ex_outdirs)
    map_outdirs = copy.copy(extract.OUTDIRS)
    extract.OUTDIRS = []
    merge_hdfs(extract,map_outdirs,L_key)

def dispatch_options(opts):
    args = []
    key_args = ['-f','--file','-k','--key','-v','--value']
    for num,key in enumerate(opts):
        if num%2==0 and key in key_args:
            args.append(key)
            args.append(opts[num+1])
    return args

if __name__=='__main__':
    parser = OptionParser()
    parser.add_option('-m','--month',dest='month',\
                       help='the month of dirs',\
                       default=False,type='str'),\
    parser.add_option('-y','--year',dest='year',\
                       help='the year of dirs',\
                       default='2015',type='str'),\
    parser.add_option('-d','--dir',dest='dir',\
                       help='keyword in directory',\
                       default=True)
    parser.add_option('-f','--file',dest='filename',\
                       help='write the dir of file',\
                       default=True)
    parser.add_option('-k','--key',dest='key',\
                        help='the key of file',\
                        type='str',default=True)
    parser.add_option('-v', '--value',dest='value',\
                        help='the value of file',\
                        type='str',default=False)
    (opts,args) = parser.parse_args()
    args = dispatch_options(sys.argv[1:])
    DATE = '%s%s'%(opts.year,opts.month)
    main(opts.dir,*args)

