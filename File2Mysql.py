#!/usr/bin/env python
#!-*- coding: utf-8 -*-


import pymysql
import re
import sys
import copy
from optparse import OptionParser 


class File2Mysql:
	emailreg = re.compile('[\w_]+@([\w]+\.[\w]+)')
	passreg = re.compile('[\w]{32}|[\w]{40}|[\w]{16}')
	ipreg = re.compile('([\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3})')
	realname = re.compile(u'[\u4e00-\u9fa5]+')

	def usage(self):
		parser = OptionParser()  
		parser.add_option("-f", "--file", dest="filename", help="Import Mysql in File") 
		parser.add_option("-p", "--type", dest="type", help="assign File type ", default="other")
		parser.add_option("-c", "--column", dest="column", help="assign column deal file")
		parser.add_option("-t", "--thread", dest="thread", help="The thread number to run", default=5)
		parser.add_option("-n", "--name", dest="name", help="table name to save", default=5)
		global options
		(options, args) = parser.parse_args() 
		if len(sys.argv) < 2:
			print parser.print_help() 
			sys.exit() 


	def dealfile(self, files, table):
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


	def Analysis(self, files):
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


	def writestruct(self, table):
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


	def writetable(self, table, data):
		sql = '''CREATE TABLE IF NOT EXISTS `{0}` (`id` int(11) NOT NULL AUTO_INCREMENT,'''.format(table)
		for line in data:
			if line == 'id':
				continue
			sql += "`{0}` varchar(100),".format(line.replace('\n', ''))
		suffix = '''PRIMARY KEY (`id`),KEY `searchku1_username_name_email` (`username`,`name`,`email`,`qq`)) 
	ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
		'''.strip()
		sql = "{0}{1}".format(sql, suffix)
		print sql
		# return sql

	def insertsql(self, table):
		sql = "insert into `{0}`(".format(table)
		temp = ""
		i = idpos = 0  # 在这里为1是判断是否为第一行
		with open(files) as f:
			for line in f.readlines():
				if line == "\n":  #遇到空行pass
					continue
				temp = "{0}) values(".format(sql[:-1]) #这里是 insert into table(asda,asd,asd) values(
				for value in line.split(sp):
					if value == 'id': #如何 列中有ID的话 pass 因为我们自己会生成ID并自增
						idpos = i  #纪录ID的位置 
						continue
					if i > 0:
						if value == line.split(sp)[idpos]:  #如何这个值等于ID位置的值的话就pass
							continue
						temp += "\"{0}\",".format(value.replace('\n','')) #把值排好
					else:
						sql += "`{0}`,".format(value.replace('\n',''))  #第一次循环的时候 把columns排好
				if i >0 :
					tmp = "{0})".format(temp[:-1]) #最后闭合括号
					print tmp  #mysql import 
				i += 1

	def main(self):
		self.usage()
	
class MysqlImport:
	def __init__(self, host, user, passwd, db='searcku'):
		self.conn = pymysql.connect(host=host, user=user,passwd=passwd,db=db)
		self.cursor = self.conn.cursor()

	def executeInsert(self, sql):
		self.cursor.execute(sql)
		self.conn.commit()

	def __del__(self):
		print 'close'
		self.cursor.close()
		self.conn.close()

if __name__ == '__main__':
	a = File2Mysql()
	a.main()
