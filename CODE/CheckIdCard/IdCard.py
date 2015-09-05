#!/usr/bin/python
#-*- coding: utf-8 -*-
#author=mia

class IdCard:
    __CONFIG = {
        'AREA': {
            "11":"北京", "12":"天津", "13":"河北", "14":"山西", "15":"内蒙古",
            "21":"辽宁", "22":"吉林", "23":"黑龙江",
            "31":"上海", "32":"江苏", "33":"浙江", "34":"安徽", "35":"福建", "36":"江西", "37":"山东",
            "41":"河南", "42":"湖北", "43":"湖南", "44":"广东", "45":"广西", "46":"海南",
            "50":"重庆", "51":"四川", "52":"贵州", "53":"云南", "54":"西藏",
            "61":"陕西", "62":"甘肃", "63":"青海", "64":"宁夏", "65":"新疆",
            "71":"台湾", "81":"香港", "82":"澳门", "91":"国外",
        },
        'ARGVS':{
            15:{'gender':'[-1]','age':'[6:8]','area':'[:2]'},
            18:{'gender':'[-2]','age':'[6:10]','area':'[:2]'}
        }
    }

    def __init__(self, idCard):
        self.__idcard = idCard
        self.D_info = {'GENDER':'null','AGE':'null','AREA':'null'}

    def getInfo(self):
        N = len(self.__idcard)
        if self.__infoVerify(N):
            self.D_info['GENDER'] = self.__infoGender(N)
            self.D_info['AGE'] = self.__infoAge(N)
            self.D_info['AREA'] = self.__infoArea(N)
        return self.D_info

    def __infoVerify(self,N):
        #验证地区,验证出生日期合法性,验证校验码
        if N == 18:
            Area,birth = self.__idcard[0:2],self.__idcard[6:14]
        elif N == 15:
            Area,birth = self.__idcard[0:2],'19'+self.__idcard[6:12]
        else:
            return ''
        if Area not in self.__CONFIG['AREA']:
            return ''
        try:
            date = birth
            self.__checkDate(date)
        except:
            return '' 
        if N == 18:
            try:
                S = (int(self.__idcard[0])+int(self.__idcard[10]))*7+(int(self.__idcard[1])+int(self.__idcard[11]))*9+(int(self.__idcard[2])+int(self.__idcard[12]))*10+(int(self.__idcard[3])+int(self.__idcard[13]))*5+(int(self.__idcard[4])+int(self.__idcard[14]))*8+(int(self.__idcard[5])+int(self.__idcard[15]))*4+(int(self.__idcard[6])+int(self.__idcard[16]))*2+int(self.__idcard[7])*1+int(self.__idcard[8])*6+int(self.__idcard[9])*3
                Y = S % 11
                M = "F"
                JYM = "10X98765432"
                M = JYM[Y]  #判断校验位
                if(M != self.__idcard[17]):   #检测ID的校验位
                     return ''
            except:
                return ''
        return 1
    
    def __checkDate(self,date):
        return Utils.DateTimeUtil(date, '%Y%m%d')
    
    def __getDate(self):
        return str(Utils.DateTimeUtil.now()).split('T')[0][:4]


    def __infoGender(self,N):
        gender_s= self.__idcard
        gender_index = '%s%s'%('gender_s',self.__CONFIG['ARGVS'][N]['gender'])
        Gender = eval(gender_index)
        if Gender !='X':
            if int(Gender)%2:
                return '男'
            return '女' 
        return '女'

    def __infoAge(self,N):
        age_s= self.__idcard
        print age_s,self.__CONFIG['ARGVS'][N]['age']
        age_index = '%s%s'%('age_s',self.__CONFIG['ARGVS'][N]['age'])
        BirthYear = eval(age_index)
        if N == 15:
            BirthYear = '19' +'%s'%BirthYear
        Age = int(self.__getDate()) - int(BirthYear)
        return Age

    def __infoArea(self,N):
        Area_s= self.__idcard
        Area_index = '%s%s'%('Area_s',self.__CONFIG['ARGVS'][N]['area'])
        Area = eval(Area_index)
        return self.__CONFIG['AREA'][Area]