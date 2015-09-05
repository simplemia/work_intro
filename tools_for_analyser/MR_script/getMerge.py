#!/usr/bin/python
#encoding=utf-8
#author:mia

import sys 
from optparse import OptionParser

CONF_OUT = {}

def define_key(rkey):
    for k in rkey.split(','):
        CONF_OUT[k] = 'NULL'

def get_output(no,coll_dict):
    for key,value in coll_dict.items():
        if value:
            CONF_OUT[key] = value
    print '%s,%s'%(no,','.join(CONF_OUT.values()))

def main():
    per_rows = '' 
    coll_dict = {}
    define_key(opts.rkey)
    for line in sys.stdin:
        rows = line.strip().split('\t')
        if per_rows and rows[0] != per_rows[0]:
            get_output(per_rows[0],coll_dict)
            define_key(opts.rkey)
            coll_dict = {}
        coll_dict[rows[-1]] = ','.join(rows[1:-1])
        per_rows = rows
    if per_rows:
        get_output(per_rows[0],coll_dict)

if __name__ == '__main__':
    try:
        parser = OptionParser()
        parser.add_option('-r','--rkey',dest='rkey',\
                            help='the key of file',\
                            type='str')
        (opts,args) = parser.parse_args()
        print '%s,%s'%('key',opts.rkey)
    except:
        opts.key = "201503,201504,201505"
        print '\
        eg1:python getMerge.py -rk "201503,201504,201505" #文件第一列作为key,报表的行属性 \
        '
    main()
