#/usr/bin/python
#-*- coding: utf-8 -*-
#author=mia

import sys


import re
import os
import math
import Utils
import Tools 
from datetime import datetime
from CONF_DIR import *
from optparse import OptionParser  


class Streaming:
    def __init__(self):
        self.CMD_ROOT = os.getcwd()                                         #文件所在目录
        H_pwd = lambda x : '/home/' + x.split('/')[2]                       #获取目录用户
        self.HADOOP_ROOT = H_pwd(self.CMD_ROOT)                                               
        self.HADOOP_MR = {'EXTRACT':('getMap.py',''),\
                        'MAP':('getMap.py',''),\
                        'MERGE':('','getMerge.py'),\
                        'COUNT':('getMap.py','uniq -c'),\
                        'UNIQ':('getMap.py','uniq')}                         #MR 字典，抽取，统计，去重
        self.INDIRS = []
        self.OUTDIRS = []
        self.DL = True
        self.EXECUTE = 1
    
    def test_dir(self,dir):
        return Tools.Hadoop.testsHadoopDir(dir,'e')

    def size_dir(self,dir):
        return Tools.Hadoop.sizeHadoopFile(dir,unit='M')

    def check_dir(self,dir):                                                #检查输入的目录
        size = self.size_dir(dir)                                           #要求目录不为空，大小5M以上 
        if dir and size>5:
            return dir
        print 'the input %s is null'%dir
        sys.exit(1)

    def py_file(self,file,*argv):                                           #处理文件路径上传hadoop
        files = '%s/MR_script/%s'%(self.CMD_ROOT,file)
        if '-f' in argv:
            argv = list(argv)                                               #python2.4
            filename = argv[argv.index('-f') + 1]
            cfile = '%s/%s'%(self.CMD_ROOT,filename)
            return (files,cfile,)
        return (files,)

    def execute_hcmd(self,argvs):
        argv = ""
        for i in argvs:
            if i.startswith('-'):
                argv += i+' '
            else:
                argv += "'%s' "%i
        return argv

    def string_MR(self,file_py,*argv):
        if argv:
            argv = self.execute_hcmd(argv)
            return 'python2.7 %s %s '%(file_py,argv)
        else:
            return 'python2.7 %s'%(file_py)

    def files_mr(self,key,*argv):                                           #处理-mapper，-reducer，-file
        scripts = ()
        mapper,reducer = self.HADOOP_MR[key]                                       #参数
        if mapper:
            scripts = self.py_file(mapper,*argv)
            mapper = self.string_MR(mapper,*argv)
        else:
            mapper = 'cat'
        if reducer:
            if reducer == 'uniq' or reducer == 'uniq -c':
                pass
            else:
                scripts += self.py_file(reducer)
                reducer = self.string_MR(reducer,*argv)
        return mapper,reducer,scripts

    def get_hdir(self,key,input):                                           #对应input的output 地址
        if isinstance(input,tuple) or isinstance(input,list):
            input = input[0]
        nums = re.findall('\d+',input)
        dir_month = '%s_%s'%(nums[-2],nums[-1])
        key = '%s_%s'%(key,input.split('/')[3])
        dir = '%s/%s/%s'%(self.HADOOP_ROOT,key,dir_month)
        return dir

    def run_MR(self,key,in_dir,*argv):                                     #执行MR程序
        if argv:
            mmap,rred,files = self.files_mr(key,*argv)
        else:
            mmap,rred,files = self.files_mr(key)
        out_dir = self.get_hdir(key,in_dir)
        params = {
            'file': files, \
            'D': {\
                'mapred.job.tracker': 'cdh246:9001',\
                'mapred.job.priority': 'HIGH',\
                'mapred.job.dir':'UnicomExtract',\
                },\
        }
        if self.test_dir(out_dir):
            Tools.Hadoop.removeHadoopFile(out_dir,execute=self.EXECUTE)
        Result = Tools.Hadoop.MapRed(in_dir,out_dir,'%s'%mmap, \
                        '%s'%rred,execute=self.EXECUTE,**params)
        if not Result['CODE']:
            return out_dir
        err = Result['STDERR']
        print err
        sys.exit(0)

    def Sqoop_export(inp):
        db = ('10.172.x.x', '1521', 'LTCX')
        userdir,passwd = 'xxx','xxx'
        dirc,table = inp
        cmd = Tools.Sqoop.Export(db,userdir,passwd,table,dirc,execute=E)

    def download_hdfs(self,dir):
        new_file = 'results/'+'.'.join(dir.split('/')[-2:])
        Tools.runCommand('hadoop fs -cat %s/* > %s '%(dir,new_file),execute=self.EXECUTE)
        return new_file
