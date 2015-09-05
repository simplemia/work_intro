#!/usr/bin/python
#encoding=utf-8
#author:mia

import sys
from optparse import OptionParser  


CONF_dict = {}

def user_dict(file_name,key,value):
    for i in open(file_name).readlines():
        rows = i.strip().split('\t')
        no = rows[key]
        cate = get_cates(rows,value)
        CONF_dict[no] = cate

def get_cates(rows,v):
    cate = ''
    if not v:
        return 'x'
    elif isinstance(v,int):
        return rows[v]
    elif isinstance(v,tuple):
        for i in v:
            cate += rows[i]+'\t'
        return cate[:-1]
    return v

def get_elements(rows,element):
    element_list = []
    element = eval(element)
    if hasattr(element, '__call__'):
        #element = lambda x: '\t'.join(x)+r'\t2-15_03'
        elements = element(rows)
        if elements:
            element_list.append(elements)
    elif isinstance(element,int):
        element_list.append(rows[int(element)])
    elif isinstance(element,tuple):
        element_list = [rows[int(i)] for i in element]
        #element_list.append(rows[int(element)])
    return element_list

def __get_elements(rows,element):
    element_list = []
    element = eval(element)
    element = lambda x:x[45] if x[2]=='''1''' else None
    elements = element(rows)
    if elements:
        element_list.append(elements)
    return element_list

def get_values(id,rows,value):
    if value:
        values = '\t'.join(get_elements(rows,value))
        if values:
            return '%s\t%s'%(id,values)
        return ''
    else:
        return id

def main(key,**kwargvs):
    #user_dict(file_name,key,value)                                 #开发人员可选模式
    #user_dict(file_name,0,(2,3))
    #user_dict(file_name,0,1)
    value = False
    if kwargvs:
        if kwargvs['filename']:
            file_name = kwargvs['filename']
            user_dict(file_name,0,'x')
            #user_dict(file_name,0,(1,2,3))
        if kwargvs['value']:
            value = kwargvs['value']
        else:
            value = ''
    for i in sys.stdin:
        rows = i.strip().split('\t')
        id = '_'.join(get_elements(rows,key))
        if CONF_dict:
            if id in CONF_dict:
                if CONF_dict[id] != 'x':
                    print get_values(id,rows,value)+'\t%s'%CONF_dict[id]
                else:
                    out = get_values(id,rows,value)
                    if out:
                        print out 
        else:
            out = get_values(id,rows,value)
            if out:
                print out   

if __name__ == '__main__':
    try:
        parser = OptionParser()
        parser.add_option('-f','--file',dest='filename',\
                           help='write the name of file',\
                           default=False)
        parser.add_option('-k','--key',dest='key',\
                            help='the key of file',\
                            type='str')
        parser.add_option('-v', '--value',dest='value',\
                            help='the value of file',\
                            type='str',default=False)
        (opts,args) = parser.parse_args()
    except:
        print '\
        eg1:python getMap.py -f 05liushi.no -k 0 #文件第一列作为key，找到用户相关属性 \
        eg2:python getMap.py -f 05liushi.no -k 0,3 -v 1 #文件第一列,第三列为key，第二列作为合并表 \
        eg3:python getMap.py -f 05liushi.no -k 0 -v 2,3 #文件第一列为key，第3,4列作为合并表 \
        '
    kwargvs = {'filename':opts.filename,'value':opts.value}
    main(opts.key,**kwargvs)
