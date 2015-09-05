#/usr/bin/python
#-*- coding: utf-8 -*-
#author=mia

import sys


import re
import os
import math
import Utils
import Tools 
import Streaming
from datetime import datetime
from datetime import timedelta
from CONF_DIR import *
from optparse import OptionParser  

E = 0

class Extract(Streaming.Streaming):
    def __init__(self):
        self.HTYPE = 'EXTRACT'
        #self.HTYPE = 'COUNT'
        self.KDIR = 'offline'
        self.INDIRS = ''
        self.YEAR,self.MONTH = self.__get_now()
        self.args = ('-k','0')
        Streaming.Streaming.__init__(self)
        Streaming.Streaming.EXECUTE = E
   
    def __get_now(self):
        year,month = datetime.now().isoformat().split('-')[:2]
        return year,month
    
    def run_by_month(self):                                            #multi_months or previous months  or single months
        print self.MONTH
        if '-' in self.MONTH:
            start,end = self.MONTH.split('-')
            month = (int(start),int(end))
            self.run_tuple(month)
        elif isinstance(self.MONTH,str):
            if self.MONTH.startswith('p'):
                month = re.sub('p','',self.MONTH)
                self.run_previous(month)
            else:
                month = int(self.MONTH)
                self.run_streaming(month)

    def run_by_kdir(self):                                              #input_dirs or input_keywords
        if isinstance(self.KDIR,list) or isinstance(self.KDIR,tuple):
            self.INDIRS = self.KDIR
            self.run_streaming()
        elif isinstance(self.KDIR,str):
            if self.KDIR.startswith('/'):
                self.INDIRS = self.KDIR
                self.run_streaming()
            elif self.KDIR in D_dirs:
                self.run_by_month()
        else:
            print 'run_by_input did not match list or str'
            sys.exit(1)
            
    def run(self,htype,date,dirs,*args):
        self.HTYPE = htype
        self.KDIR = dirs
        self.YEAR,self.MONTH = date[:4],date[4:]
        self.args = args
        self.run_by_kdir()
        if self.DL:
            for out in self.OUTDIRS:
                self.download_hdfs(out)
        #Sqoop_export(sqoop_in)                                             #导出标签数据到oracle表中
    
    def run_streaming(self,*month):
        if month:
            date = (self.YEAR,month)
            self.get_indir(date)
        out_dir = Streaming.Streaming.run_MR(self,self.HTYPE,self.INDIRS,*self.args)
        self.OUTDIRS.append(out_dir)
        self.INDIRS = ''
    
    def run_tuple(self,months):
        start,end = months
        for month in range(start,end+1):
            self.run_streaming(month)

    def run_previous(self,month):
        now = datetime.now() 
        subyear = int(float(self.MONTH)/now.month)
        submonth = int(float(self.MONTH)%now.month)
        MONTH = now.month
        if self.YEAR != subyear:
            if MONTH != submonth:
                months = (1,MONTH)
                self.run_tuple(months)
            self.YEAR -= 1
            MONTH = 12
        else:
            if MONTH != submonth:
                months = (MONTH-submonth,MONTH)
                self.run_tuple(months)
            else:
                month = MONTH
                self.run_streaming(month)
    
    def get_indir(self,date):
        year,month = date
        if self.KDIR in D_dirs:
            self.INDIRS = D_dirs[self.KDIR]%(year,'%02d'%month)
    
def dispatch_options(opts):
    args = []
    key_args = ['-f','--file','-k','--key','-v','--value']
    for num,key in enumerate(opts):
        if num%2==0 and key in key_args:
            args.append(key)
            args.append(opts[num+1])
    return args

if __name__=='__main__':
    #try:
    #print sys.argv[1:]
    parser = OptionParser()
    parser.add_option('-m','--month',dest='month',\
                       help='the month of dirs',\
                       default=True,type='str'),\
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
    extract = Extract()
    extract.run('EXTRACT',opts.year,opts.month,opts.dir,*args)
    #except:
    #    print ' \n \
    #    eg1:python EXTRACT.py -f 05liushi.no -k 0 -m 06 -d cate #文件第一列作为key，找到用户相关属性,06月数据,cate标签数据\n \
    #    eg2:python EXTRACT.py -f 05liushi.no -k 0,3 -v 1 -m 02-06 -d cate#文件第一列,第三列为key，第二列作为合并表\n  \
    #    eg3:python EXTRACT.py -f 05liushi.no -k 0 -v 2,3 -m 06 -d cate #文件第一列为key，第3,4列作为合并表\n  \
    #    目录介绍见CONF_Dir\
    #    '
