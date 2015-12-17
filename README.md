##Mysql数据库导入脚本

###支持目前大多数社工库格式文件，导入一个固定格式的表中。用于信息查询。

####开发者可以增加正则表达式，提高识别效率
>三种导入方式

* -f 指定导入文件
* －s 指定分隔符 
* －c 指定列名
* －p 指定文件类型 

1.python File2Mysql.py -f data.txt -s ,  默认自动识别表结构并建立索引。此时表结构是固定的。

2.python File2Mysql.py -f data.txt -s , -p sqlmap 这种是sqlmap脱裤后的脚本可以直接导入，并按照他的表结构建立索引

3.python File2Mysql.py -f data.txt -s , -c username,qq,email,idcard 指定列名,但是注意列的顺序。指定这些其他的自动加入other字段中
