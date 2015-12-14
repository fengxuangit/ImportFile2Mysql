#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import pymysql
import re
import sys
import copy
# sp = sys.argv[1]
sp = ','
# files = '/Users/apple/python/test/163.txt'
emailreg = re.compile('[\w_]+@([\w]+\.[\w]+)')
passreg = re.compile('[\w]{32}|[\w]{40}|[\w]{16}')
ipreg = re.compile('([\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3})')

realname = re.compile(u'[\u4e00-\u9fa5]+')
sqlfile = "test.sql"

def dealfile(files, table):
	conn = MysqlImport(host='192.168.108.130',user='mysql', passwd='mysql123', db='searchku')
	conn.executeInsert(writestruct(table)) # 建表

	formt = Analysis(files)
	prefix = "insert into {0}(".format(table)
	suffix = "("
	for key in sorted(formt):
		prefix += "`{0}`,".format(key)
	prefix = "{0}`other`) values(".format(prefix)
	with open(files) as f:
		for line in f.readlines():
			value = ""
			data = line.split(sp)
			temp = copy.copy(data)
			for key in sorted(formt):
				pos = formt[key]
				value += "\"{0}\",".format(ReplaceAll(data[pos]))
				temp.pop(int(pos))
			data = ReplaceAll(str(temp))
			oneline = "{0} {1}\"{2}\")".format(prefix, value, data)	 #处理好sql语句 准备写入文件
			conn.executeInsert(oneline)


def ReplaceAll(text):
	filters = {',':'', '[':'', ']':'', '"':''}
	for sech, rep in filters.items():
		text = text.replace(sech, rep)
	return text

def GetOneline(files):
	with open(files) as f:
		data = f.readline()
		return data.split(sp)

def Analysis(files):
	i = 0
	form = {}
	for line in GetOneline(files):
		if emailreg.search(line):
			form['email'] = i
		if passreg.search(line):
			form['password'] = i
		if ipreg.search(line):
			form['ip'] = i
		if realname.search(line):
			form['name'] = i
		i+=1
	return form

def writestruct(table):
	sql = '''
	CREATE TABLE IF NOT EXISTS `{0}` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`username` varchar(45) DEFAULT NULL,
`password` varchar(45) DEFAULT NULL,
`email` varchar(45) DEFAULT NULL,
`name` varchar(20) DEFAULT NULL,
`bankid` int(11) DEFAULT NULL,
`idcard` int(11) DEFAULT NULL,
`address` varchar(50) DEFAULT NULL,
`qq` int(11) DEFAULT NULL,
`ip` varchar(45) DEFAULT NULL,
`phone` int(11) DEFAULT NULL,
`website` varchar(50) DEFAULT NULL,
`other` text,
PRIMARY KEY (`id`),
KEY `searchku1_username_name_email` (`username`,`name`,`email`,`qq`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
	'''.format(table).strip()
	return sql

def is_chinese(word):
	reg = re.compile(u'[\u4e00-\u9fa5]+')
	match = reg.search(word)
	if match:
		return True
	else:
		return False

def main():
	inputfile = sys.argv[1]
	dealfile(inputfile, 'search2')


class MysqlImport:
	def __init__(self, host, user, passwd, db='searcku'):
		self.conn = pymysql.connect(host=host, user=user,passwd=passwd,db=db)
		self.cursor = self.conn.cursor()

	def executeInsert(self, sql):
		self.cursor.execute(sql)
		self.conn.commit()

	def GetfetchAll(self):
		pass

	def __del__(self):
		print 'close'
		self.cursor.close()
		self.conn.close()

if __name__ == '__main__':
	main()




