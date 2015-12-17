#!/usr/bin/env python
#!-*- coding: utf-8 -*-


import pymysql
import re
import sys
import copy
import logging
from os.path import basename
from optparse import OptionParser 


class MysqlImport:
	def __init__(self, host, user, passwd, db='searcku'):
		self.conn = pymysql.connect(host=host, user=user,passwd=passwd,db=db)
		self.cursor = self.conn.cursor()

	def executeInsert(self, sql):
		print sql
		try:
			self.cursor.execute(sql)
		except:
			logging.critical("sql execute error")
			sys.exit()
		self.conn.commit()
		logging.info("execute sql success")

	def __del__(self):
		self.cursor.close()
		self.conn.close()

class File2Mysql:
	emailreg = re.compile('[\w_]+@([\w]+\.[\w]+)')
	passreg = re.compile('[\w]{32}|[\w]{40}|[\w]{16}')
	ipreg = re.compile('([\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3})')
	realname = re.compile(u'[\u4e00-\u9fa5]+')
	conn = None
	def columns(self,files):
		with open(files) as f:
			for line in f.readlines():
				data =  line.split(options.split)
				break
		return data

	def ReplaceAll(self, text):
		filters = {',':'', '[':'', ']':'', '"':'', '\n':'', '\r':''}
		for sech, rep in filters.items():
			text = text.replace(sech, rep)
		return text

	def GetOneline(self, files):
		with open(files) as f:
			data = f.readline()
			return data.split(options.split)

	def MixFile(self, files, table):
		self.conn = MysqlImport(host='virtual.ub',user='mysql', passwd='mysql123', db='searchku')
		self.conn.executeInsert(self.writestruct(table)) # 建表
		logging.info("CREATE table  success")
		formt = self.Analysis(files)
		prefix = "insert into `{0}`(".format(table)
		suffix = "("
		for key in sorted(formt):
			prefix += "`{0}`,".format(key)
		prefix = "{0}`other`) values(".format(prefix)
		with open(files) as f:
			for line in f.readlines():
				value = ""
				data = line.split(options.split)
				temp = copy.copy(data)
				for key in sorted(formt):
					pos = formt[key]
					value += "\"{0}\",".format(self.ReplaceAll(data[pos]))
					temp.pop(int(pos))
				data = self.ReplaceAll(str(temp))
				oneline = "{0} {1}\"{2}\")".format(prefix, value, data)	 #处理好sql语句 准备写入文件
				self.conn.executeInsert(oneline)
		logging.info("done!!!")

	def Analysis(self, files):
		i = 0
		form = {}
		for line in self.GetOneline(files):
			if self.emailreg.search(line):
				form['email'] = i
			if self.passreg.search(line):
				form['password'] = i
			if self.ipreg.search(line):
				form['ip'] = i
			if self.realname.search(line):
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


	def SqlmapFile(self, files, table, columns=None):
		data = columns if columns else self.columns(files)
		sql = '''CREATE TABLE IF NOT EXISTS `{0}` (`id` int(11) NOT NULL AUTO_INCREMENT,'''.format(table)
		for line in data:
			if line == 'id':
				continue
			sql += "`{0}` varchar(100),".format(line.replace('\n', ''))
		suffix = '''PRIMARY KEY (`id`),KEY `searchku1_username_name_email` (`username`,`name`,`email`,`qq`)) 
	ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
		'''.strip()
		sql = "{0}{1}".format(sql, suffix)
		self.conn = MysqlImport(host='virtual.ub',user='mysql', passwd='mysql123', db='searchku')
		self.conn.executeInsert(sql) # TODO 打印信息
		logging.info("CREATE table  success")
		self.insertsql(files, table)
		logging.info("done!!!")

	def insertsql(self,files, table, columns=None):
		sql = "insert into `{0}`(".format(table)
		temp = ""
		i = idpos = 0  # 在这里为1是判断是否为第一行
		with open(files) as f:
			for line in f.readlines():
				if line == "\n":  #遇到空行pass
					continue
				temp = "{0}) values(".format(sql[:-1]) #这里是 insert into table(asda,asd,asd) values(
				for value in line.split(options.split):
					if value == 'id': #如何 列中有ID的话 pass 因为我们自己会生成ID并自增
						idpos = i  #纪录ID的位置 
						logging.debug("found id and remember this position")
						continue
					if i > 0:
						if value == line.split(options.split)[idpos]:  #如何这个值等于ID位置的值的话就pass
							logging.warning("drop the `id` line ")
							continue
						temp += "\"{0}\",".format(self.ReplaceAll(value)) #把值排好
					elif columns != None and type(columns) == list:  #判断是否存在自定义列名	
						for column  in columns:
							sql += "`{0}`,".format(column) 
						break
					else:
						sql += "`{0}`,".format(self.ReplaceAll(value)) 
				if i >0 :
					tmp = "{0})".format(temp[:-1]) #最后闭合括号
					self.conn.executeInsert(tmp)  #mysql import todo 打印信息
				i += 1


	def DiyColumns(self):
		columns = []
		if options.column == None:
			return None
		else:
			for line in options.column.split(options.split):
				columns.append(line)
			self.SqlmapFile(options.file, name, columns)

	def usage(self):
		parser = OptionParser()  
		parser.add_option("-f", "--file", dest="file", help="Import Mysql in File") 
		parser.add_option("-p", "--type", dest="type", help="assign File type ", default="other")
		parser.add_option("-c", "--column", dest="column", help="assign column deal file")
		parser.add_option("-t", "--thread", dest="thread", help="The thread number to run", default=5)
		parser.add_option("-n", "--name", dest="name", help="table name to save")
		parser.add_option("-s", "--split", dest="split", help="the split symbol", default=",")
		parser.add_option("-l", "--log", dest="logfile", help="save log file path", default="File2Mysql.log")
		global options
		(options, args) = parser.parse_args() 
		if len(sys.argv) < 2:
			print parser.print_help() 
			sys.exit()

	def logformat(self):
		if options.logfile == None:
			options.logfile = options.name[:options.name.find('.')] + '.log'
		logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=options.logfile,
                filemode='w')
		console = logging.StreamHandler()
		console.setLevel(logging.INFO)
		formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s %(message)s')
		console.setFormatter(formatter)
		logging.getLogger('').addHandler(console)

	def main(self):
		self.usage()
		self.logformat()
		logging.info("start handle with {0} ".format(options.name))
		if options.name == None:
			name = basename(options.file[:options.file.find('.')])
		else:
			name = options.name
		if options.type == 'other':
			if options.column == None:
				self.MixFile(options.file, name)
			else:
				self.DiyColumns()
		else:
			self.SqlmapFile(options.file, name)

if __name__ == '__main__':
	a = File2Mysql()
	sys.exit(a.main())
