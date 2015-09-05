## Laplapce  ##

Laplapce is a tool for analysis coworkers to get data in various common conditions from hadoop.
    
    eg：get offline users' behaviors from 201501 to 201504 

        python Run.py -f key/05liushi.no -k 0 -m 01-04 -d cate
        -k the key for streaming in the HDFS files ;
        -f 05offline.no(the telephone no of the offline users);
        -m the range of months;
        -cate keyword from CONF_DIR.py(a dict for alias directories of HDFS);
            

##Intro
###MR_script

**getMap.py**
> it is a all-purpose map script. it can be controlled by some certain arguments. such as extracting the certain keys in file, using lambda function to get needed line in the certain conditions.
> 
> the argument '-k' could not be empty, the argument '-f' designates the key file, argument '-v' designates the value of map；
	
	hadoop fs -cat /home/manman.xu/MERGE_MAP_EXTRACT_offline/2015_03/p* |python2.7 getMap.py -k "lambda x: \"\t\".join(x) if x[2]==\"1\" else None "

**getMerge.py**

> After getting the results of previous script, it is a simple collected reduce script to collect one key's mul-months data, and generate a csv report or a hdfs file. the results is as following.
> 
> the argument '-r' designates the sequence of the output. 

	hadoop fs -cat /home/manman.xu/MAP_EXTRACT_offline/2015_*/p* |python2.7 getMerge.py -r "2015_01,2015_02,2015_03,2015_04"

> results

	key,2015_01,2015_02,2015_03,2015_04
	13002900389,微信,手机QQ,None,微信
	13002900568,手机QQ,微信,None,None
	13002901538,YouTube,微信,None,None
	13002901569,微信,Facebook,微信,None
	13002901775,手机QQ,QQ音乐,微信,糗事百科

	
###Dispatch_script

**CONF_DIR.py**  

> simplifing using the common directions, using the key rename the directions.

	D_dirs={
	'cate':'/data/category/basic_union/merge/merge/%s/%s',   #标签目录
	'contact':'/data/model/contacts/raws/%s/%s/unicom/p*',   #语音朋友圈
	'loc_dealed':'/data/category/location_info/raws/%s/%s/*',#位置更新去噪目录
	'offline':'/data/model/offline/reason/%s/%s/*',          #流失原因
	}


**Streaming.py**
> it is class running the hadoop streaming with some common purposes. such executing a streaming scripts with certain map or shell commands, then you will get a full command that could run in hadoop, and download it in local place. 

> aulmatically deal with the direction of output
> 
> support different ways to deal with files；

**Extract.py** 
>  it inherits the class of Streaming.Streaming, and call the fuctions to accomplisg the series streaming scripts. 
	
	python2.7 Extract.py -m '7-8' -d 'loc' -f lac_id.conf -k '4'
> output 

    hadoop fs -test -e '/home/manman.xu/EXTRACT_location_info/2015_07'
    hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/MR_script/getMap.py' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/lac_id.conf' -mapper "python2.7 getMap.py -f 'lac_id.conf' -k '4'  " -reducer "NONE"  -output '/home/manman.xu/EXTRACT_location_info/2015_07' -input '/data/category/location_info/raws/2015/07/*'

    hadoop fs -test -e '/home/manman.xu/EXTRACT_location_info/2015_08'
    hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/MR_script/getMap.py' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/lac_id.conf' -mapper "python2.7 getMap.py -f 'lac_id.conf' -k '4'  " -reducer "NONE"  -output '/home/manman.xu/EXTRACT_location_info/2015_08' -input '/data/category/location_info/raws/2015/08/*'
	
	hadoop fs -cat /home/manman.xu/EXTRACT_location_info/2015_07/* > results/EXTRACT_location_info.2015_07
	hadoop fs -cat /home/manman.xu/EXTRACT_location_info/2015_08/* > results/EXTRACT_location_info.2015_08

**Reports.py**  
it is a scripts to call the class of Extract.py to collect mul-months results, then generate corresponding reports.
Fri-part：each line of each input plus its corresponding date on the end of line；Sed-part：using getMerge.py script to collect results by different key.
	
	python2.7 Reports.py -f 'liushi0102_no.sample' -k '0' -v '1' -m '03' -d offline

> output
	
	/yjcom/app/hadoop-2.3.0-cdh5.1.0/bin-mapreduce1/hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/getMap.py' -mapper "python2.7 getMap.py -k 'lambda x: x[0]+\"\t2015_03\" if x[1]==\"1\" else None" '  " -reducer "NONE"  -output '/home/manman.xu/MAP_EXTRACT_offline/2015_03' -input '/home/manman.xu/EXTRACT_offline/2015_03'
		
	hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/mutil_combine_reports/tools_for_analyser/getMerge.py' -mapper "cat" -reducer "python2.7 getMerge.py -r '2015_03'  "  -output '/home/manman.xu/MERGE_MAP_EXTRACT_offline/2015_03' -input '/home/manman.xu/MAP_EXTRACT_offline/2015_03'



=================================================================================================================

#Laplapce分析人员工具（hdfs）#


##简介
> 帮助分析人员快速抽取数据，验证数据准确性，免于编写脚本，操作hadoop文件；
> 
> 程序封装了hdfs 目录，年月信息，streaming 中的map 和 reduce 脚本。

	CONF_DIR.py  目录封装成关键字；
	getMap.py 	 通用map 程序，实现key,value 条件查询，以及文件关键字过滤；
	getMerge.py  通用reduce 程序，实现汇总用户多月报表程序；
	Streaming.py 封装文件上传，解析hdfs执行参数，下载，导出等常用操作；
	Extract.py	 解决封装问题，hdfs 目录，年月信息，streaming的调度问题；
	Reports.py   调用Extract.py 实现多月汇总，数据产出用户报表；        

## 模块介绍 ##
###MR_script

**getMap.py**
> 封装map 输出，解析参数，实现抽取文件中的关键字段，map中k,v条件抽取，关键字文件中k-v 匹配；
> 
> 传入参数-k 默认不为空，-f为存放关键字的文件，-v 输出的value值；

	
	处理情况1： 一批号码文件，对数据进行筛选，-v存在时，输出kv 列，-v不存在时，输出该行数据；（-f liushi.no -k 0）;
	处理情况2： 对数据第几列为0，lambda 函数条件查询，-k/-v  为"lambda x:\"\t\".join(x)+\"\t2015_03\" ";
	处理情况3： 一批号码文件,且满足value 符合条件，-f liushi.no -k 0 -v "lambda x: x[15] if x[2]==\"1\" else None" ;


**getMerge.py**

> 封装reduce 输出过程，对一个key的value进行收集，输出为该用户多月的报表；
> 
> 传入参数-r 默认不为空，为汇总数据的列名，制定输出报表的格式；


	处理情况：-r 参数为文件日期，放在文件的第一行列名

###Dispatch_script

**CONF_DIR.py**  

> 简化目录的使用，使用关键字调用目录

**Streaming.py**
> 封装streaming常用参数，简化输入参数，
	
	参数1：类别决定，map,red常用文件，map 用于抽取，red 用户汇总多月数据，计算，去重，抽取，条件查询；
	参数2：输入目录，根据输入目录确定输出目录；
	参数3：可变参数，为map程序，提供执行参数；

> 自动化了output过程，自动化处理了map red多变，以及需要上传文件的判断；

> 下载hdfs上输出结果到本地，导出到oracle，判断文件是否存在

**Extract.py** 
>  封装常使用目录为关键字调用（在CONF_DIR.py文件中），封装时间范围解析;
>  
>  继承streaming中的run_MR，执行一定时间范围的streaming map reduce 程序;
	
	参数1：hdfs目录，关键字调用对应的目录；
	参数2：执行时间段，可以指定执行单月(-m 7)；执行多月执行(-m 1-6);执行近半年数据执行(-m p6）；

**Reports.py**  对多月的结果进行汇总的streaming程序；程序分文两个部分：
	
	第一部分：按照月份，将各文件为每行结果的最后一列，输出为日期；
	第二部分：同一个key 的，讲value 按照规定的日期顺序输出（初始化为NULL）；

> 使用getMerge.py 封装了报表的过程，只需要制定输出结果的顺序；
##例子##


**eg1:**处理08月，位置数据，匹配基站数据(lac_id.conf),匹配文件中的第5列

> python2.7 Extract.py -m '8' -d 'loc' -f lac_id.conf -k '4'

**eg2:**处理01月到04月，在文件36Y_0506month.no 里面的用户，他们的标签id是以222开头的，输出第5，8列；

	python2.7 Extract.py -m '01-04' -d cate -f 36Y_0506month.no -k '3'  -v 'lambda x:\"_\".join((x[4],x[7])) if x[4].startswith(\"222\") else None'



**验证：**map的函数是否可以正常运行
	
	hadoop fs -cat /home/manman.xu/MERGE_MAP_EXTRACT_offline/2015_03/p* |python2.7 getMap.py -k "lambda x: \"\t\".join(x) if x[2]==\"1\" else None "

<span style="color:red">注意：python2.7 getmap.py -k "lambda x: \"\t\".join(x) if x[2]==\"1\" else none " 使用"" shell 内部可以解析字符串（\"） </span> 

> 其他常用方法介绍
		
	cat app_top_01-04 |python2.7 localMap.py -f step2_voice_high.no -k 0 |grep -v None |awk -F '\t' '{print $5,$2,$3,$4}'|sort |uniq -c |sort -nr |head -n 20

	cat app_top_01-04 |python2.7 getMap.py -f step2_voice_high.no -k "lambda x:x[0] if x[3]==\"1\" else None"|head 

