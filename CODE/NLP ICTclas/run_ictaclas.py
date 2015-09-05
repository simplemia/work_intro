#!/usr/bin/python
#_*_encode=utf-8_*_

from ctypes import cdll,create_string_buffer,addressof,c_char_p,string_at
ld=cdll.LoadLibrary('/home/mia/ICTCLA/API/libICTCLAS50.so')
Init=getattr(ld,'_Z12ICTCLAS_InitPKc')
Init('/home/mia/ICTCLA/API/')
ImportDict=getattr(ld,'_Z22ICTCLAS_ImportUserDictPKci9eCodeType')
Process=getattr(ld,'_Z24ICTCLAS_ParagraphProcessPKciPc9eCodeTypeb')
Exit=getattr(ld,'_Z12ICTCLAS_Exitv')
 
print 'before:'
szText = open('test.txt','r').read()
cntText=len(szText)
buf=create_string_buffer(cntText*6)
rlen=Process(szText,cntText,buf,3,True)
print buf.value
Exit()
