redis_merger
============

Redis的RDB日志汇聚重放工具

工作原理
============
1."把自己模拟成一个Redis"	
2."向多个master发出sync指令，请求同步RDB文件"		
3."解析RDB生成Redis指令，发送给目标Redis"	