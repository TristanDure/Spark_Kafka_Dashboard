## 实时交易数据分析平台
### 序章
+ 公司的带数据培训实战营要做出一个东西来，作为新组长试试做一个朴素的demo，课题来源是[Spark课程实验案例：Spark+Kafka构建实时分析Dashboard](http://dblab.xmu.edu.cn/post/8274/)；
+ 本来想用springboot做的，框架都搭好了，卡在了socketio上，正好看到有个flask+socketio+echarts的CPU监控工具（不知道哪个博客是真的原创，就放这个[链接](https://www.cnblogs.com/hhh5460/p/7397006.html)）比较值得借鉴，就改成了相同的框架；
+ 改成python后卡最长时间的json的序列化和反序列化，最后参考了（同样不知道是不是原的）[链接](https://www.cnblogs.com/magic8sky/p/10475704.html)，在此基础上写了一个反序列化为对象List的方法；
+ 最后包含两个项目，主项目是Flask 项目python_dashboard，辅助项目是Spark流计算数据处理项目DashboardDataProcessing；
+ 已经完成了水博客的工作，参考[Spark+Kafka构建实时数据分析面板](https://www.jianshu.com/p/c3e33f03c0dc)


### 准备工作
1. 会python，flask，scala，基本的前端语法；

2. 部署一个单机的kafka，mac可以参考[链接](https://www.jianshu.com/p/23b75520a632)；

3. 部署一个单机spark；

4. 我的环境是python3.7(anaconda版)+pyCharm，需要第三方库包括Flask，Flask-SocketIO，Kafka；

5. scala项目环境是scala12.10+IDEA，依赖库在pom.xml中有；

6. 最后测试的浏览器最好用chrome或safari，360极速浏览器可能会吃掉CSS；

    

### 使用方法

####  当前的半成品

1. 使用PyCharm或其他编辑器打开项目python_dashboard，用IDEA打开项目DashboardDataProcessing；

2. 修改python_dashboard项目中的setting.py，根据实际情况修改配置；

3. 修改DashboardDataProcessing中KafkaProcessing.scala第20行，把设置setting.py的路径；

4. 运行DashboardDataProcessing项目KafkaProcessing.scala，然后运行python_dashboard项目中app.py；

#### 成品构想

步骤1和2同半成品

3. 使用maven或者其他方式打包DashboardDataProcessing项目，并测试能否在本地正常运行该项目，如果出错后面就不用看了；
4. 修改setting.py中JAR_PATH为打包后DashboardDataProcessing的实际地址，app.py第26行取消注释；
5. 运行app.py即可启动项目，有需要的可以将python_dashboard一起打包；

#### 如果不使用spark-streaming流计算

将setting.py中CONSUMER_TOPIC和PRODUCER_TOPIC的值设为一样，则默认使用python_dashboard中内置的慢速数据处理方法；



### 工作流程
1. 定时生成若干条随机数据，转为Json；

2. Producer传输这些数据；

3. SparkStreaming接收这些数据；

4. SparkStreaming处理好数据并重新发送；

5. Consumer接收数据并传输给socket-io；

6. 前端通过echarts绘制实时折线图；

   

### 展示页面
1. 总体展示
2. 4个城市对比
3. 男女对比
4. 4种支付方式对比
5. 6种消费类型对比



### 未解之谜

1. 前端页面刷新时间理论上应该是2s，实际上流计算环境是8s，过一会稳定后才会变回2s；
2. scala项目打包后本地无法运行，当前问题是Exception in thread "streaming-start" java.lang.NoClassDefFoundError: org/apache/spark/kafka010/KafkaConfigUpdater；



### 后续工作

1. 添加数据库的支持，处理过数据的努力就不会木大了；
2. 找一个数据输入接口，随机生成数据不太靠谱；
