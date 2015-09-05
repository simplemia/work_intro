## get data from HDFS for analysis coworkers ##

This is a tool for analysis coworkers to freely use data from HDFS and for developer to simplly get conditional data for reports 
    
    eg：get fakecard users' behaviors from 201501 to 201504 

        python Run.py -f key/05liushi.no -k 0 -m 01-04 -d cate
        -k the key for streaming in the HDFS files ;
        -f 05fakecard.no(the telephone no of the offline users);
        -m the range of months;
        -cate keyword from CONF_DIR.py(a dict for alias directories of HDFS);
	
	eg:get the 45th rows which the 3th rows equeals string 1 from directory of offline,
		python2.7 Day_tt.py -k 'lambda x:x[45] if x[2]==\"1\" else None' -m 03-05 -d offline
            

### Intro  ###

Three models ：

1. configuration(CONF_DIR.py),including a dict for alias directories of HDFS；
2. hadoop streaming mapper(extract/getNo.py)，it is a script of mapper；
3. main script(Run.py) automatically accomplish to dispatch arguments, \
	generate the streaming scripts ,run the streaming, \
	download the file from hdfs to the local directory(results). 

### Example ###

python Run.py -f 05liushi.no -k 0 -m 06 -d catepython Run.py -f 05liushi.no -k 0 -m 06 -d cate

hadoop fs -test -e '/home/manman.xu/extract_basic_union/2015_06'

hadoop fs -rm -r -f '/home/manman.xu/extract_basic_union/2015_06'

hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/tools_for_analyser/extract/getNo.py' -file '/data03/manman.xu/public/tools_for_analyser/05liushi.no' -mapper 'python2.7 getNo.py -f 05liushi.no -k 0' -reducer 'NONE'  -output '/home/manman.xu/extract_basic_union/2015_06' -input '/data/category/basic_union/merge/merge/2015/06'

hadoop fs -cat /home/manman.xu/extract_basic_union/2015_06/* > results/extract_basic_union.2015_06


=======================================================================


## 自定义抽取字段程序执行 ##

主要实现，抽取特定关键字的用户xx数据,帮助数据分析人员方便提取数据，简化参数。

    
    比如：抽取05月养卡用户的1-4月数据，需要执行

        python Run.py -f key/05fakecard.no -k 0 -m 01-04 -d cate
        -k 是指电话号码所对应的列，0指第一个字段;
        -f 05liushi.no 流失用户的电话号码文件;
        -m 是指01-04月份数据;
        -cate 在CONF_DIR.py 配置对应的文件和关键字;
            

### 程序介绍 ###

分为三个模块：

1. 目录字段，CONF_DIR.py，字典内配置，目录与便于程序选取的关键字；
2. MR 抽取字段程序，getNo.py，extract目录下，负责由主函数上传到HDFS上面执行程序；
3. 主程序，Run.py，自动调配参数，生成streaming 脚本，自动运行，下载hdfs上文件到本地目录（results）；

###例子###
python Run.py -f 05liushi.no -k 0 -m 06 -d catepython Run.py -f 05liushi.no -k 0 -m 06 -d cate

hadoop fs -test -e '/home/manman.xu/extract_basic_union/2015_06'

hadoop fs -rm -r -f '/home/manman.xu/extract_basic_union/2015_06'

hadoop jar /yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar -D mapred.job.priority='HIGH' -D mapred.job.tracker='cdh246:9001' -D mapred.job.dir='UnicomExtract' -file '/data03/manman.xu/public/tools_for_analyser/extract/getNo.py' -file '/data03/manman.xu/public/tools_for_analyser/05fakecard.no' -mapper 'python2.7 getNo.py -f 05fakecard.no -k 0' -reducer 'NONE'  -output '/home/manman.xu/extract_basic_union/2015_06' -input '/data/category/basic_union/merge/merge/2015/06'

hadoop fs -cat /home/manman.xu/extract_basic_union/2015_06/* > results/extract_basic_union.2015_06