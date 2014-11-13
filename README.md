redis_merger
============

Redis的RDB日志汇聚重放工具

这是干什么的
============
1.将线上的session数据从老的Redis集群中，复制到一个新的Redis集群中	


工作原理
============
1.把自己模拟成一个Redis		
2.向多个master发出sync指令，请求同步RDB文件			
3.解析RDB生成Redis指令，发送给目标Redis		

