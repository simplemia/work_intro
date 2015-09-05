#!/usr/bin/python2.7
##-*- coding: utf-8 -*-

import re                       # for judge the name correct
import os                       # for get size of the file
import sys

import getpass

import subprocess

import time

import random
import socket

import ftplib                   # package that deal with the ftp

import Utils                    # some useful class and functions

if sys.version > '2.6':
    from hashlib import md5
else:
    import md5

# function: conditions ? a : b
def ThreeOperator(conditions, a, b):
    return (conditions and [a] or [b])[0]


# the import imformation
def hdfsDirectory(template, startDate, endDate = None, flag = 'DAY'):
    if not isinstance(startDate, Utils.DateTimeUtil):
        return ''

    if flag == 'DAY':
        return __hdfsDirectoryDay(template, startDate, endDate)

    elif flag == 'MONTH':
        return __hdfsDirectoryMonth(template, startDate, endDate)


def __hdfsDirectoryDay(template, startDate, endDate = None):
    if not endDate or not isinstance(endDate, Utils.DateTimeUtil):
        endDate = Utils.DateTimeUtil.dayDelta_s(startDate, days = 1)

    # one month or the one year
    if startDate.firstDay() and endDate.firstDay():
        month = "%02d"%(startDate.month())
        if (endDate.month() - startDate.month() == 1) and \
           endDate.year() == startDate.year():
            return template%("%04d"%(startDate.year()), month, '*')

        if startDate.firstDay(flag = 'YEAR') and endDate.firstDay(flag = 'YEAR') and \
           (endDate.year() - startDate.year() == 1):
            return template%("%04d"%(startDate.year()), '*', '*')

    # return the list
    dirs, sd = [], startDate.copy()
    while( sd < endDate ):
        dirs.append(template%("%04d"%(sd.year()), "%02d"%(sd.month()), "%02d"%(sd.day())))
        sd.tomorrow()       # add one day

    if len(dirs) == 1:
        return dirs[0]

    return dirs


def __hdfsDirectoryMonth(template, startDate, endDate = None):
    if not endDate or not isinstance(endDate, Utils.DateTimeUtil):
        endDate = Utils.DateTimeUtil.dayDelta_s(startDate, months = 1)

    # one month or the one year
    if startDate.firstMonth() and endDate.firstMonth() and \
       (endDate.year() - startDate.year() == 1):
        return template%("%04d"%(startDate.year()), '*')

    # return the list
    dirs, sd = [], startDate.copy()
    while( sd < endDate ):
        dirs.append(template%("%04d"%(sd.year()), "%02d"%(sd.month())))
        sd.next()       # add one month

    if len(dirs) == 1:
        return dirs[0]

    return dirs


# judge if exec or just template the string
def __execOrTemplate(string, params):
    if callable(string):
        return string(params)

    return string%(params)


# generate Params
def __makeParams(params, pParams):
    for pms in pParams:
        cParams, levels = params, pms.split('/')

        lastLevel = levels[-1]
        for level in levels[:-1]:
            if level in cParams: cParams = params[level]

        if lastLevel in cParams:
            cParams[lastLevel] = __execOrTemplate(cParams[lastLevel],
                                                  pParams[pms])

    return params


def initResultsCommand(code = 0, command = '', stderr = '',
                           stdout = '', stdin = '',
                           environ = os.environ.copy()):
    return __initResultsCommand__(code = code, command = command,
                                  stderr = stderr, stdout = stdout,
                                  stdin = stdin, environ = environ)


def __initResultsCommand__(code = 0, command = '', stderr = '',
                           stdout = '', stdin = '',
                           environ = os.environ.copy()):
    return {
        'CODE': code,
        'STDERR': stderr,
        'STDOUT': stdout,
        'STDIN': stdin,
        'COMMAND': command,
        'DATE': Utils.DateTimeUtil.now(),
        'ENVIRON': environ,
        'USER': getpass.getuser(),
    }


def __printRunCommandResults__(t, printString, results):
    if results['CODE'] == 0:
        print('Execute Command SUCCESS [%s]: %s'%(t, printString))
    else:
        print('Execute Command FAILED [%s]: %s\n%s'%(t, printString, results['STDERR']))


# execute a command
def runCommand(command, execute = True, exit = False,
               environ = os.environ.copy(),
               stdin = None, stdout = None, stderr = None,
               printString = '', printResults = False, **params):
    # the environment of the execution
    basicEnv = os.environ.copy()
    basicEnv.update(environ)

    # if the command is list or tuple object
    # we use the space to join the list and turn it to a string
    if isinstance(command, (list, tuple)):
        command = ' '.join(command)

    results = __initResultsCommand__(environ = basicEnv,
                                     command = ThreeOperator(printString, printString,
                                                             command))

    # the command must be string till this step
    if not isinstance(command, str):
        results['CODE'] = 1
        return results

    # print the command need to be execute
    printString = ThreeOperator(printString, printString, command)
    print("Execute Command [%s]: "%(Utils.DateTimeUtil.now()) + printString)

    # need to execute the command
    if execute == True:
        child = subprocess.Popen(command, shell = True, env = basicEnv,
                                 stdout = subprocess.PIPE, stderr = subprocess.PIPE)

        if isinstance(stdin, str):
            results['STDIN'] = stdin
            out = child.communicate(stdin)
        else: out = child.communicate()

        # record the stdout and stderr string
        results['STDOUT'] = out[0]
        results['STDERR'] = out[1]

        # execute function to deal with the stdout
        if hasattr(stdout, '__call__'):
            for line in out[0].splitlines():
                stdout(line)

        # execute function to deal with the stderr
        if hasattr(stderr, '__call__'):
            for line in out[1].splitlines():
                stderr(line)

        results['CODE'] = child.returncode

        # print the results of the command
        if printResults: __printRunCommandResults__(Utils.DateTimeUtil.now(),
                                                    printString,
                                                    results)

    # return the result of the execution of the command
    return results


def Dstring(params):
    DString = ""
    # must be tuple or list, the format is ((NAME, value), (NAME, value), ... )
    # or [(NAME, value), (NAME, value), ... ]
    if isinstance(params, (tuple, list)):
        for value in params:
            if len(value) >= 2:
                if isinstance(value[1], (int, float)):
                    DString += "-D %s=%s "%(value[0], value[1])
                else:
                    DString += "-D %s='%s' "%(value[0], value[1])
            # or the type of params['D'] is dict
            # the format is {NAME: 'value'}
    elif isinstance(params, dict):
        for key in params:
            if isinstance(params[key], (int, float)):
                DString += "-D %s=%s "%(key, params[key])
            else:
                DString += "-D %s='%s' "%(key, params[key])


    return DString


# generate the sequcence, the sequence must be in prefer
def __generateCommandSequences(keys, sequences):
    orders = []

    lefts = list(set(keys) - set(sequences))
    for seq in sequences:
        if seq in keys:
            orders.append(seq)

    orders.extend(lefts)

    return tuple(orders)


# return the key value string
def paramsString(params, promot, pattern = '', sequences = ()):
    string = ''

    orders = params.keys()
    if sequences:
        orders = __generateCommandSequences(params.keys(), sequences)

    for key in orders:
        if isinstance(params[key], (tuple, list)):
            for value in params[key]:
                if pattern:
                    string += "%s "%(pattern%(promot, key, value))
                else:
                    string += "%s%s '%s' "%(promot, key, value)
        else:
            if pattern:
                string += "%s "%(pattern%(promot, key, params[key]))
            else:
                string += "%s%s '%s' "%(promot, key, params[key])

    return string


def delRequireParams(params, requires):
    for field in requires:
        if field in params:
            del params[field]

    return params


# the sqoop class, used to generate the command
class Sqoop:
    CONFIG = {
        'CONNECT': "jdbc:oracle:thin:@%s:%s:%s",
        'COMMAND': '/yjcom/app/sqoop-1.4.4-cdh5.1.0/bin/sqoop',

        'DELIMITER': {
            'IMPORT': '\\t',
            'EXPORT': '\\t',
        },

        'MAPS': {
            'IMPORT': 1,
            'EXPORT': 1,
        },
    }

    def __init__(self):
        pass

    @classmethod
    def Export(self, database, username, password, table,
               targetDir, execute = False, **params):
        requires = ('connect', 'username', 'password', 'export-dir', 'table')

        command = "%s export "%(self.CONFIG['COMMAND'])

        # consider the -D opt
        if 'D' in params:
            command += Dstring(params['D'])
            del params['D']     # delete the 'D'

        command += "--connect %s "%(self.CONFIG['CONNECT']%(database))

        # add the basic information, username, password, targetDir
        command += "--username %s "%(username)
        command += "--password %s "%(password)
        command += "--export-dir %s "%(targetDir)

        # export data into table
        command += "--table %s "%(table)

        # del some parameters, connect, username, etc
        params = delRequireParams(params, requires)

        # num-mappers in the params
        if 'num-mappers' not in params:
            command += "--num-mappers %s "%(self.CONFIG['MAPS']['EXPORT'])

        if 'input-fields-terminated-by' not in params:
            command += "--input-fields-terminated-by '%s' "%(self.CONFIG['DELIMITER']['EXPORT'])

        # not consider the -D value
        command += paramsString(params, '--')

        return runCommand(command, execute = execute, printResults = True)


    # import data from oracle
    # database, username, password, targetDir is must parameters
    # the dict parameters format is:
    # 'NAME': (value1, value2) is transformat into -NAME 'value1' -NAME 'value2'
    # 'NAME': 'value' is tranformated into -NAME 'value'
    # 'D' :{'NAME': value} is transformat into -D NAME=value
    @classmethod
    def Import(self, database, username, password,
               targetDir, execute = False, remove = False, **params):
        command = "%s import "%(self.CONFIG['COMMAND'])

        # consider the -D opt
        if 'D' in params:
            command += Dstring(params['D'])
            del params['D']     # delete the 'D'

        command += "--connect %s "%(self.CONFIG['CONNECT']%(database))

        # add the basic information, username, password, targetDir
        command += "--username %s "%(username)
        command += "--password %s "%(password)
        command += "--target-dir %s "%(targetDir)

        if remove and Hadoop.testsHadoopDir(targetDir, tp = 'e'):
            Hadoop.removeHadoopFile(targetDir, execute = execute)

        # num-mappers in the params
        if 'num-mappers' not in params:
            command += "--num-mappers %s "%(self.CONFIG['MAPS']['IMPORT'])

        if 'query' in params and '$CONDITIONS' not in params['query']:
            if 'where' in params['query']:
                params['query'] += ' and \$CONDITIONS'
            else:
                params['query'] += ' where \$CONDITIONS'

        if 'fields-terminated-by' not in params:
            command += "--fields-terminated-by '%s' "%(self.CONFIG['DELIMITER']['IMPORT'])

        # not consider the -D value
        command += paramsString(params, '--', """%s%s "%s" """)

        return runCommand(command, execute = execute, printResults = True)


# upper class for Sqoop, like Streaming class
class Sqooping:
    def __init__(self, configs, **params):
        if not self.__checkRequires(('TARGET', 'DB', 'USER', 'PASSWD'),
                                configs):
            raise

        self.__target = configs['TARGET']
        self.__db = configs['DB']
        self.__user, self.__passwd = configs['USER'],\
                                     configs['PASSWD']

        # judge the is import or export
        # is type is export, then record the table
        self.__type = 'IMPORT'
        if 'TABLE' in configs:
            self.__type = 'EXPORT'
            self.__table = configs['TABLE']

        # define the other params
        # detail information please reference
        # Sqoop.Import or Sqoop.Export
        if 'PARAMS' in configs and isinstance(configs['PARAMS'], dict):
            self.__params = configs['PARAMS']
        else: self.__params = {}

        # back up the configs
        self.__configs = configs


    def __checkRequires(self, requires, configs):
        for r in requires:
            if r not in configs: return False

        return True


    def setTarget(self, target):
        self.__target = target

    def getTarget(self):
        return self.__target

    def getConfigs(self):
        return self.__configs.copy()

    @classmethod
    def __execOrTemplate(self):
        return globals()['__execOrTemplate']

    @classmethod
    def __makeParams(self):
        return globals()['__makeParams']

    # the struct of the params is same as the default params
    # the default params use the add and update the params
    def updateParams(self, **params):
        # update the params, if 'D' in params
        if 'D' in params and 'D' in self.__params:
            self.__params.update(params)
            del params['D']

        # update the other options
        self.__params.update(params)


    def prepareSqoop(self, tParams = (), pParams = {},
                     tbParams = ()):
        self.__target = self.__execOrTemplate()(self.__target, tParams)

        # if type is 'export', update the table
        if self.__type == 'EXPORT':
            self.__table = self.__execOrTemplate()(self.__table, tbParams)

        # update the params
        if pParams:
            self.__params = self.__makeParams()(self.__params, pParams)


    def importing(self, execute = False, remove = False):
        results = Sqoop.Import(self.__db, self.__user, self.__passwd,
                               self.__target, execute = execute,
                               remove = remove, **self.__params)

        return results


    def exporting(self, execute = False):
        results = Sqoop.Export(self.__db, self.__user, self.__passwd,
                               self.__table, self.__target, execute = execute,
                               **self.__params)

        return results


    def imported(self, tParams = (), pParams = {},
                 execute = False, remove = False):
        self.prepareSqoop(tParams = tParams, pParams = pParams)

        return self.importing(execute = execute)


    def exported(self, tParams = (), pParams = {},
                 tbParams = (), execute = False):
        self.prepareSqoop(tParams = tParams, pParams = pParams,
                          tbParams = tbParams)

        return self.exporting(execute = execute)


class Sqluldr:
    # some constants
    CONFIG = {
        'ADDENV': {
            'LD_LIBRARY_PATH': '/opt/oracle/instantclient_10_2',
            'ORACLE_HOME': '/opt/oracle/instantclient_10_2',
            'TNS_ADMIN': '/opt/oracle/instantclient_10_2',
            'ORACLE_IC_HOME': '/opt/oracle/instantclient_10_2',

            'PATH': '/usr/java/default/bin:/usr/java/default/bin:'
            '/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:'
            '/opt/oracle/instantclient_10_2:'
            '/opt/oracle/instantclient_10_2:/opt/oracle/'
            ':/opt/oracle/instantclient_10_2:/opt/oracle/'
            ':/opt/oracle/instantclient_10_2:/opt/oracle/'
            ':/opt/oracle/instantclient_10_2:/opt/oracle/',
        },

        'COMMAND': 'sqluldr2',

        'FIELD': {
            'IMPORT': '0x09',
        }
    }

    def __init__(self):
        pass

    # Import data from the oracle database
    @classmethod
    def Import(self, basic, query, outFile, execute = False, **params):
        command = self.CONFIG['COMMAND']

        command += " %s/%s@%s "%(basic)
        command += """query="%s" """%(query)
        command += "file=%s "%(outFile)

        # set the delimiter of the fields
        if 'field' not in params:
            command += 'field=%s '%(self.CONFIG['FIELD']['IMPORT'])

        for key in params:
            if isinstance(params[key], (tuple, list)):
                for value in params[key]:
                    command += "%s='%s' "%(key, value)
            else:
                command += "%s='%s' "%(key, params[key])

        return runCommand(command, execute = execute,
                          environ = self.CONFIG['ADDENV'],
                          printResults = True)


class SqlPlus:
    def __init__(self):
        pass


class Tez:
    def __init__(self, configs):
        pass

    def tez(self):
        pass


class Streaming:
    __CONFIG = {
        'TMP': '/yjtest',
        '': {
        },
    }

    def __init__(self, configs):
        if 'INPUT' not in configs:
            raise

        # store the current configs
        self.__configs = configs.copy()

        # define the input directory
        self.__inputs = configs['INPUT']

        # define the output directory
        if 'OUTPUT' in configs:
            self.__output = configs['OUTPUT']
        else:                   # generate the temp directory
            now = Utils.DateTimeUtil.now()
            self.__output = "%s/%s/%s/%s/%s"%(self.__CONFIG['TMP'],
                                              getpass.getuser(),
                                              now.format("%Y-%m-%d"),
                                              now.format("%H"),
                                              UUID.uuid())

        # define the mapper and reducer
        # default mapper is 'cat' if not define the mapper
        # default reducer is 'NONE' if not define the reducer
        self.__mapper, self.__reducer = 'cat', 'NONE'
        if 'MAPPER' in configs: self.__mapper = configs['MAPPER']
        if 'REDUCER' in configs: self.__reducer = configs['REDUCER']

        # define the other params
        # the detail information of the params please reference
        # Hadoop.MapRed define parmas
        if 'PARAMS' in configs and isinstance(configs['PARAMS'], dict):
            self.__params = configs['PARAMS']
        else: self.__params = {}


    # the struct of the params is same as the default params
    # the default params use the add and update the params
    def updateParams(self, **params):
        # update the params, if 'D' in params
        if 'D' in params and 'D' in self.__params:
            self.__params.update(params)
            del params['D']

        # update the other options
        self.__params.update(params)

    def setInput(self, inputs):
        self.__inputs = inputs

    def setOutput(self, output):
        self.__output = output

    def setMapper(self, mapper):
        self.__mapper = mapper

    def setReducer(self, reducer):
        self.__reducer = reducer

    def getInput(self):
        return self.__inputs

    def getOutput(self):
        return self.__output

    def getMapper(self):
        return self.__mapper

    def getReducer(self):
        return self.__reducer

    # return the configs
    def configs(self):
        return self.__configs.copy()

    @classmethod
    def __execOrTemplate(self):
        return globals()['__execOrTemplate']

    @classmethod
    def __makeParams(self):
        return globals()['__makeParams']

    # prepare streming
    def prepareStream(self, iParams = (), oParams = (),
                      mParams = (), rParams = (), pParams = {}):
        if iParams:
            if isinstance(self.__inputs, (tuple, list)):
                self.__inputs = [self.__execOrTemplate()(v, iParams[i])
                                 for (i, v) in enumerate(self.__inputs)]
            else: self.__inputs = self.__execOrTemplate()(self.__inputs,
                                                          iParams)

        # generate output
        self.__output = self.__execOrTemplate()(self.__output, oParams)

        # mapper and reducer
        self.__mapper = self.__execOrTemplate()(self.__mapper, mParams)
        self.__reducer = self.__execOrTemplate()(self.__reducer, rParams)

        # generate the params
        if pParams:
            self.__params = self.__makeParams()(self.__params, pParams)


    # must first call the prepareStrem, then call this function
    def streaming(self, remove = False, execute = False, skipper = True):
        results = Hadoop.MapRed(self.__inputs, self.__output, self.__mapper,
                                self.__reducer, remove = remove, execute = execute,
                                skipper = skipper, **self.__params)

        return results

    # the wapper of the streaming command, not changing the object value
    def stream(self, iParams = (), oParams = (), mParams = (),
               rParams = (), pParams = {} ,remove = False, execute = False,
               skipper = True):
        self.prepareStream(iParams = iParams, oParams = oParams,
                           mParams = mParams, rParams = rParams, pParams = pParams)

        return self.streaming(remove = remove, execute = execute,
                              skipper = skipper)

        # newJob = Streaming(self.configs())
        # newJob.prepareStream(iParams, oParams, mParams, rParams)
        # return newJob.streaming(remove = remove, execute = execute, skipper = skipper)


# the Hadoop Class, used to generate command
class Hadoop:
    def __init__():
        pass

    CONFIG = {
        'PATH': '/yjcom/app/hadoop-2.3.0-cdh5.1.0/share/hadoop/mapreduce1/contrib/streaming/hadoop-streaming.jar',
        'COMMAND': '/yjcom/app/hadoop-2.3.0-cdh5.1.0/bin-mapreduce1/hadoop',
        'ORDERS': ('libjars', 'outputformat', ),

        'UNIT': {
            'B': 1024 ** 0,
            'K': 1024 ** 1,
            'M': 1024 ** 2,
            'G': 1024 ** 3,
            'T': 1024 ** 4,
        },
    }


    @classmethod
    def MR(self):
        pass

    # mainly running a streaming Job, if you wan't to run a jar job
    # just use the MR function
    # inDir, outDir, mapper, and reducer must be as a parameter
    # the dict parameters format is:
    # 'NAME': (value1, value2) is transformat into -NAME 'value1' -NAME 'value2'
    # 'NAME': 'value' is tranformated into -NAME 'value'
    # 'D' :{'NAME': value} is transformat into -D NAME=value
    @classmethod
    def MapRed(self, inDir, outDir, mapper = 'cat',
               reducer = 'cat', path = '',
               skipper = False, remove = False,
               execute = False, **params):
        if path == '':
            path = self.CONFIG['PATH']
        cmdString = "%s jar %s "%(self.CONFIG['COMMAND'], path)

        # consider the -D opt
        if 'D' in params:
            cmdString += Dstring(params['D'])
            del params['D']     # delete the 'D'

        # not consider the -D value
        cmdString += paramsString(params, '-', sequences = self.CONFIG['ORDERS'])

        # add the mapper and reducer
        cmdString += '-mapper "%s" -reducer "%s" '%(mapper, reducer)

        cmdString += self.__AddMapRedOutput(outDir, remove, execute = execute)

        inputString = self.__AddMapRedInput(inDir, skipper and execute)
        if not inputString:
            return __initResultsCommand__(code = 1, command = cmdString,
                                          stderr = 'All the Input Direcotry is not exists')
        # add the input
        cmdString += inputString

        return runCommand(cmdString, execute = execute, printResults = True)


    # the function that add the input dirs
    # if skipper = True, then skip the dirs that not exists
    @classmethod
    def __AddMapRedInput(self, inDirs, skipper):
        inputs = ''

        # add the in and out directory
        if isinstance(inDirs, (list, tuple)):
            inputs += ' '.join(["-input '%s'"%(idir) for idir in inDirs \
                                if not skipper or self.testsHadoopDir(idir)])
        else:
            if skipper and not self.testsHadoopDir(inDirs):
                # if all the target dir is not exists, then return the empty string
                return ''

            inputs += "-input '%s' "%(inDirs)

        return inputs


    # the function that deal with the output
    # if define the remove, if the target directory exists
    # then remove the target directory
    @classmethod
    def __AddMapRedOutput(self, outDir, remove, execute = False):
        outputs = ''

        if remove and self.testsHadoopDir(outDir):
            self.removeHadoopFile(outDir, execute = execute)

        outputs += " -output '%s' "%(outDir)

        return outputs


    # remove a File or Directory in HDFS
    # skipTrash: remove the file permanently
    # force: remove the file by force
    # recursive: remove directory
    # execute: set True, execute the command, False just print the Command
    @classmethod
    def removeHadoopFile(self, remDir, skipTrash = False, force = True,
                         recursive = True, execute = False):
        cmdRem = "%s fs -rm "%(self.CONFIG['COMMAND'])

        # remove the directory
        if recursive:
            cmdRem += "-r "
        # remove the directory by force
        if force:
            cmdRem += "-f "
        # remove permanetly
        if skipTrash:
            cmdRem += "-skipTrash "

        cmdRem += "'%s'"%(remDir)

        return runCommand(cmdRem, execute = execute, printResults = True)


    # get the file size of the hadoop
    # if the target is the direcotry, then return the size of the directory
    @classmethod
    def sizeHadoopFile(self, fileName, unit = 'M', human = False):
        # if the unit not is B, K, M, T, P, then return the Bytes
        if unit not in self.CONFIG['UNIT']:
            unit = 'B'

        totalSize = [0]
        command = "%s fs -du -s %s"%(self.CONFIG['COMMAND'], fileName)

        def addSize(line):
            totalSize[0] = line.split()[0]

        results = runCommand(command, stdout = addSize, execute = True)

        if results['CODE'] != 0:
            return -1

        return Utils.unitChange(totalSize[0], unitIn = 'B',
                                unitOut = 'M', human = human)


    # create a dir
    @classmethod
    def mkdirHadoop(self, dirName, parent = True, execute = True):
        command = "%s fs -mkdir "%(self.CONFIG['COMMAND'])

        # if set the parent value
        if parent:
            command += "-p "

        command += "'%s'"%(dirName)

        # execute the command
        return runCommand(command, execute = execute)


    # judge a dir is exists in Hadoop File System
    @classmethod
    def testsHadoopDir(self, dirName, tp = 'e'):
        command = "%s fs -test -%s "%(self.CONFIG['COMMAND'], tp)

        command += "'%s'"%(dirName)
        results = runCommand(command)

        if results['CODE']:
            return False

        return True


    # put a file into the hdfs
    @classmethod
    def putFileToHadoop(self, fileName, hdfsPath, execute = True, **params):
        command = "%s fs -put "%(self.CONFIG['COMMAND'])

        if 'D' in params:
            command += Dstring(params['D'])
            del params['D']

        command += "%s %s"%(fileName, hdfsPath)

        return runCommand(command, execute = execute, printResults = True)


    # download a file from the hdfs
    @classmethod
    def getFileFromHadoop(self, hdfsPath, localPath = '', execute = True):
        command = "%s fs -get "%(self.CONFIG['COMMAND'])

        command += "%s %s "%(hdfsPath, localPath)

        return runCommand(command, execute)


    # copy a file between the Hdfs
    @classmethod
    def copyHadoopFiles(self, sourcePath, targetPath, execute = True):
        command = "%s fs -cp "%(self.CONFIG['COMMAND'])

        command += "%s %s "%(sourcePath, targetPath)

        return runCommand(command, execute = execute, printResults = True)


    # move a file in hdfs
    @classmethod
    def moveHadoopFiles(self, sourcePath, targetPath, execute = True):
        command = "%s fs -mv "%(self.CONFIG['COMMAND'])

        command += "%s %s "%(sourcePath, targetPath)

        return runCommand(command, execute = execute, printResults = True)


    # list all the files of a dir
    # give the absolute path or relative path
    @classmethod
    def listHadoopDir(self, listDir, path = 'ABSOLUTE', execute = True):
        command = "%s fs -ls %s"%(self.CONFIG['COMMAND'], listDir)

        files = []

        def addFile(line):
            fields = line.split()
            if len(fields) == 8:
                files.append(fields[-1])

        results = runCommand(command, execute = execute, stdout = addFile)

        if results['CODE']:
            return False

        if path == 'RELATIVE':
            return [os.path.basename(p) for p in files]

        return files


# execute the pig command
class Pig:
    # the class constant
    CONFIG = {
        'COMMAND': 'pig',
    }

    def __init__(self):
        pass


    # execute pig command
    @classmethod
    def pig(self, name, execute = False, **params):
        command = "%s "%(self.CONFIG['COMMAND'])

        # consider the -D opt
        if 'D' in params:
            command += Dstring(params['D'])
            del params['D']     # delete the 'D'

        # not consider the -D value
        command += paramsString(params, '', pattern = '-param %s%s=%s')

        command += " %s"%(name)

        runCommand(command, execute = execute, printResults = True)


# program of sending email
class SMail:
    CONFIG = {
        'COMMAND': 'python /data/tools/send_mail.py ',
    }

    def __init__(self):
        pass

    @classmethod
    def sendMail(self, mail_address, topic, content, execute = True):
        command = self.CONFIG['COMMAND']

        content = re.sub('`' , ' ', content)
        content = re.sub("'" , "\'", content)
        content = re.sub('"' , '\"', content)

        command += """ -m '%s' """%(mail_address)
        command += """ -t '%s' """%(topic)
        command += """ -c "%s" """%(content)

        return runCommand(command, execute = execute,
                          printString = '%s -m %s -t %s -c ...'%(self.CONFIG['COMMAND'],
                                                                 mail_address,
                                                                 topic))


class Local:
    # The constant value of different units
    CONFIG = {
        'UNIT': {
            'B': 1024 ** 0,
            'K': 1024 ** 1,
            'M': 1024 ** 2,
            'G': 1024 ** 3,
            'T': 1024 ** 4,
        },
    }

    # get the size of directory or files
    # default return the bytes
    # The name can be directory or a list of files or a single File
    # the pattern that the names must be statisticed
    # now cann't support the rescurve the directory
    @classmethod
    def getSize(self, names, pattern = '',
                unit = 'K', recursive = False):
        size = 0                # the total size

        # get the all the names that needed to be calculated
        totalNames = []
        if isinstance(names, (tuple, list)):
            totalNames.extend(names)
        elif isinstance(names, str):
            totalNames.append(names)

        for name in totalNames:
            if not re.search(pattern, name): # if the name is not statistified the pattern
                continue

            if os.path.isdir(name): # the directory
                size += self.__getDirSize(name, pattern,
                                          recursive = recursive)
            elif os.path.isfile(name): # the regular the file
                size += os.path.getsize(name)

        return size / self.CONFIG['UNIT'][unit[0].upper()] # return the user need unit


    # the input must be a dirName
    @classmethod
    def __getDirSize(self, dirName, pattern = '', recursive = False):
        size = 0

        for name in os.listdir(dirName):
            if not re.search(pattern, name):
                continue

            absName = os.path.join(dirName, name)
            if os.path.isdir(absName): # for now, not support the rescurve
                if recursive:        # the rescurve
                    size += self.__getDirSize(absName, pattern, recursive)
                else:           # just think all the directory is zero
                    size += 0
            elif os.path.isfile(absName): # the regular file
                size += os.path.getsize(absName)

        return size             # the total size, byte is unit


    @classmethod
    def __isPattern(self, pattern, string):
        if not pattern:
            return False

        if re.search(pattern, string):
            return True

        return False


    @classmethod
    def files(self, name, pattern = '',
              recursive = False, directory = False):
        results = set()

        currentPath = os.path.abspath(os.path.curdir)
        absName = os.path.join(currentPath, name)

        # the input file is not exist just return
        if not os.path.exists(name):
            return results

        # if is directory
        if os.path.isdir(name):
            listFiles = os.listdir(name)
            for f in listFiles:
                if self.__isPattern(pattern, os.path.basename(f)):
                    continue

                # recusive for directory
                if recursive:
                    results |= self.files(os.path.join(name, f),
                                          pattern = pattern,
                                          recursive = recursive,
                                          directory = directory)

                # need directory
                elif directory:
                    results.add(os.path.join(name, f))

                # only need file
                elif not os.path.isdir(f):
                    results.add(os.path.join(name, f))

        # just a file
        elif not self.__isPattern(pattern, os.path.abspath(name)):
            results.add(name)

        # print(results)
        return results


    @classmethod
    def mkdir(self, dirName, parent = True):
        # file exists, do nothing
        if os.path.exists(dirName):
            return

        if parent:
            os.makedirs(dirName)
        else:
            os.mkdir(dirName)


    # concat some files into one File
    # if not given the outputFile, then generate
    @classmethod
    def concatFiles(self, dirName, pattern = ''):
        pass


# The class of generate the uuid in python
class UUID:
    def __init__(self):
        pass


    # generate the uuid for python
    @classmethod
    def uuid(self, *args ):
        """
        Generates a universally unique ID.
        Any arguments only create more randomness.
        """
        t = int( time.time() * 1000 )
        r = int( random.random() * 100000000000000000 )
        try:
            a = socket.gethostbyname( socket.gethostname() )
        except:
            # if we can't get a network address, just imagine one
            a = random.random() * 100000000000000000
        data = str(t) + ' ' + str(r) + ' ' + str(a) + ' ' + str(args)
        data = md5.md5(data).hexdigest()

        return data


class UniqFileName:
    def __init__():
        pass

    # generate a time name of fileNames
    @classmethod
    def timeName():
        pass


    # generate a fileName of uuid
    @classmethod
    def uuidName():
        pass


class FTPUtil:
    __CONFIG = {
        'PORT': '21',
    }

    def __init__(host = '', user = '', passwd = '', login = True):
        pass

    def login(user = '', passwd = ''):
        pass

    def download(self, fileName, target, relogin = True):
        pass

    def __downloadHelp(self, fileName, target):
        pass

    def delete(fileName, relogin = True):
        pass

    def list(directory):
        pass
