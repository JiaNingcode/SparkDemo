## 今日任务

```
1. 了解spark
2. 熟悉spark相关观念
3. spark环境部署
```

## 教学重点

```
1. spark部署，yarn模式的两种方式的对比
2. 作业提交流程以及参数设置
3. 集群启动流程
```

## 第一章 认识是Spark

### 1.1 Spark引入

**回顾Hadoop**

Apache Hadoop软件库是一个允许使用简单编程模型跨计算机集群处理大型数据集合的框架，其设计的初衷是将单个服务器扩展成上千个机器组成的一个集群为大数据提供计算服务，其中每个机器都提供本地计算和存储服务。

**hadoop中的的优缺点**

高可靠性：Hadoop按位存储和处理数据的能力值得人们信赖

高扩展性：Hadoop是在可用的计算机集簇间分配数据并完成计算任务的，这些集簇可以方便地扩展到数以千计的节点中。

高效性：Hadoop能够在节点之间动态地移动数据，并保证各个节点的动态平衡，因此处理速度非常快。

高容错性：Hadoop能够自动保存数据的多个副本，并且能够自动将失败的任务重新分配。

 缺点：

不适合低延迟数据访问。

无法高效存储大量小文件。

不支持多用户写入及任意修改文件除了

这些Hadoop2.x中我们还提供了Yarn资源调度系统,专门用来进行资源的调度使用

**Hadoop的模块**

Hadoop自诞生以来，主要出现了Hadoop1、Hadoop2、Hadoop3三个系列多个版本。

Hadoop2的三大核心组件分别是HDFS、MapReduce、Yarn。目前市面上绝大部分企业使用的是Hadoop2。

Hadoop2的一个公共模块和三大核心组件组成了四个模块，简介如下：

1,、Hadoop Common：为其他Hadoop模块提供基础设施。

2、HDFS：具有高可靠性、高吞吐量的分布式文件系统。

3、MapReduce：基于Yarn系统，分布式离线并行计算框架。

4、Yarn：负责作业调度与集群资源管理的框架。

### 1.2 Spark是什么

官网http://spark.apache.org/

![Uploading image.png…]()


Spark是一种快速、通用、可扩展的大数据分析引擎，2009年诞生于加州大学伯克利分校AMPLab，2010年开源，2013年6月成为Apache孵化项目，2014年2月成为Apache顶级项目。项目是用Scala进行编写。

Spark 官网将Spark 定义为一个大型可扩展数据的**快速**和**通用**处理引擎。

首先，Spark 采用了先进的DAG执行引擎，支持循环数据流和内存计算，使得 Spark 速度更快，在内存中的速度是Hadoop MR的百倍，在磁盘上的速度是Hadoop MR的十倍(官网数据) 。

其次，Spark 是一个通用的处理引擎， 被设计用来做批处理、迭代运算、交互式查询、流处理、机器学习等。

另外，Spark 易用，可以用Scala、Java、Python、R等开发分布式应用，Spark 提供了大量高级API，方便开发。

最后，Spark 集成了多种数据源，并且可以通过local、Yarn、Mesos、Standalone（Spark 提供的部署方式）等各种模式运行。

### 1.3 Hadoop Spark 对比

#### 框架比较

|                   | hadoop                                   | spark                                |
| ----------------- | ---------------------------------------- | ------------------------------------ |
| **起源**          | 2005                                     | 2009                                 |
| 起源地            | MapReduce (Google) Hadoop (Yahoo)        | University of California, Berkeley   |
| **数据处理引擎**  | Batch                                    | Batch                                |
| **处理**          | Slower than Spark and Flink              | 100x Faster than Hadoop              |
| **编程语言**      | Java, C, C++, Ruby, Groovy, Perl, Python | Java, Scala, python and R            |
| **编程模型**      | MapReduce                                | Resilient distributed Datasets (RDD) |
| **Data Transfer** | Batch                                    | Batch                                |
| **内存管理**      | Disk Based                               | JVM Managed                          |
| **延迟**          | HIgh                                     | Medium                               |
| **吞吐量**        | Medium                                   | High                                 |
| **优化机制**      | Manual                                   | Manual                               |
| **API**           | Low-level                                | High-level                           |
| **流处理支持**    | NA                                       | Spark Streaming                      |
| **SQL支持**       | Hive, Impala                             | SparkSQL                             |
| **Graph 支持**    | NA                                       | GraphX                               |
| **机器学习支持**  | NA                                       | SparkML                              |
|                   |                                          |                                      |

#### 处理流程比较

MR中的迭代:

![图片14](./image/图片14.png)

Spark中的迭代:

![图片15](./image/图片15.png)

1、spark把运算的中间数据存放在内存，迭代计算效率更高；mapreduce的中间结果需要落地，需要保存到磁盘，这样必然会有磁盘io操做，影响性能。

2、spark容错性高，它通过弹性分布式数据集RDD来实现高效容错，RDD是一组分布式的存储在节点内存中的只读性质的数据集，这些集合是弹性的，某一部分丢失或者出错，可以通过整个数据集的计算流程的血缘关系来实现重建；mapreduce的话容错可能只能重新计算了，成本较高。

3、spark更加通用，spark提供了transformation和action这两大类的多个功能api，另外还有流式处理sparkstreaming模块、图计算GraphX等等；mapreduce只提供了map和reduce两种操作，流计算以及其他模块的支持比较缺乏。

4、spark框架和生态更为复杂，首先有RDD、血缘lineage、执行时的有向无环图DAG、stage划分等等，很多时候spark作业都需要根据不同业务场景的需要进行调优已达到性能要求；mapreduce框架及其生态相对较为简单，对性能的要求也相对较弱，但是运行较为稳定，适合长期后台运行。

### 1.4 Spark 组件

![2019-09-29_132749](./image/spark1.jpg)

**Spark Core**

实现了 Spark 的基本功能，包含任务调度、内存管理、错误恢复、与存储系统 交互等模块。Spark Core 中还包含了对弹性分布式数据集(resilient distributed dataset，简称RDD)的 API 定义。 

**Spark SQL**

是 Spark 用来操作结构化数据的程序包。通过 Spark SQL，我们可以使用 SQL 或者 Apache Hive 版本的 SQL 方言(HQL)来查询数据。Spark SQL 支持多种数据源，比 如 Hive 表、Parquet 以及 JSON 等。 

**Spark Streaming**

是 Spark 提供的对实时数据进行流式计算的组件。提供了用来操作数据流的 API，并且与 Spark Core 中的 RDD API 高度对应。 

**Spark MLlib**

提供常见的机器学习(ML)功能的程序库。包括分类、回归、聚类、协同过滤等，还提供了模型评估、数据 导入等额外的支持功能。 

**Spark GraphX**

GraphX在Spark基础上提供了一站式的数据解决方案，可以高效地完成图计算的完整流水作业。GraphX是用于图计算和并行图计算的新的（alpha）Spark API。通过引入弹性分布式属性图（Resilient Distributed Property Graph），一种顶点和边都带有属性的有向多重图，扩展了Spark RDD。

### 1.5 Spark特点

#### 快

与Hadoop的MapReduce相比，Spark基于内存的运算要快100倍以上，基于硬盘的运算也要快10倍以上。Spark实现了高效的DAG执行引擎，可以通过基于内存来高效处理数据流。

![图片4](./image/图片4.png)

#### 易用

Spark支持Java、Python和Scala的API，还支持超过80种高级算法，使用户可以快速构建不同的应用。而且Spark支持交互式的Python和Scala的shell，可以非常方便地在这些shell中使用Spark集群来验证解决问题的方法。

![图片6 ](./image/图片6 .png)

#### 通用

Spark提供了统一的解决方案。Spark可以用于批处理、交互式查询（Spark SQL）、实时流处理（Spark Streaming）、机器学习（Spark MLlib）和图计算（GraphX）。这些不同类型的处理都可以在同一个应用中无缝使用。Spark统一的解决方案非常具有吸引力，毕竟任何公司都想用统一的平台去处理遇到的问题，减少开发和维护的人力成本和部署平台的物力成本。

![图片5 ](./image/图片5 .png)

#### 兼容性

Spark可以非常方便地与其他的开源产品进行融合。比如，Spark可以使用Hadoop的YARN和Apache Mesos作为它的资源管理和调度器，器，并且可以处理所有Hadoop支持的数据，包括HDFS、HBase和Cassandra等。这对于已经部署Hadoop集群的用户特别重要，因为不需要做任何数据迁移就可以使用Spark的强大处理能力。Spark也可以不依赖于第三方的资源管理和调度器，它实现了Standalone作为其内置的资源管理和调度框架，这样进一步降低了Spark的使用门槛，使得所有人都可以非常容易地部署和使用Spark。此外，Spark还提供了在EC2上部署Standalone的Spark集群的工具。

![图片7](./image/图片7.png)

## 第二章 spark 部署

### 2.1 Spark集群部署

#### Spark部署模式

**Local** 多用于本地测试，如在eclipse，idea中写程序测试等。

**Standalone**是Spark自带的一个资源调度框架，它支持完全分布式。

**YarnHadoop**生态圈里面的一个资源调度框架，Spark也是可以基于Yarn来计算的。

**Mesos**资源调度框架。

#### 机器准备

环境预准备，至少三台机器互通互联，免密登录，时间同步，安装好JDK1.8。

#### 安装包下载

![](./image/21003272-2479-46A5-A617-1DACA027F80E.png)

**注意**：本是spark2.2.0需要的hadoop环境最好是2.7(含)版本以上

其他下载方式：

<http://archive.apache.org/dist>

https://github.com/apache/spark

### 2.2 Standalone模式

#### 集群搭建

集群组成使用了4台节点,在教学中可以根据自己的节点适当减少worker即可

~~~shell
基本条件:同步时间、免密登录、关闭防火墙、安装JDK1.8
1.上传安装包到hadoop01
2.将文件解压到指定的目录
tar -zxvf spark-2.2.0-bin-hadoop2.7.tgz -C /opt/software/

3.跳转到安装路径进入到conf进行配置
cd /opt/software/spark-2.2.0-bin-hadoop2.7/
cd conf/

3.1修改conf目录下的env文件
mv spark-env.sh.template spark-env.sh
vi spark-env.sh
在文件的末尾添加
export JAVA_HOME=/opt/software/jdk1.8.0_191 JDK安装路径
export SPARK_MASTER_IP=hadoop01 主节点IP(写节点名地址必须是节点 写IP地址必须是IP)
(这个的配置决定spark-shell时 --master中spark://地址的配置(hadoop01或IP地址))
export SPARK_MASTER_PORT=7077 主节点端口号(内部通信)

3.2修改slaves.template文件添加从节点
mv slaves.template slaves
vi slaves
内容(根据自己的节点适当删减):
hadoop02
hadoop03
hadoop04

4.分发配置好的内容到其他节点:
scp -r ./spark-2.2.0-bin-hadoop2.7/ root@hadoop04:$PWD
ps:0后面进行修改 2,3,4

配全局环境变量(选配好处:可以在任意位置使用bin下脚本，如spark-shell和sparkSubmit):
vi /etc/profile
export SPARK_HOME=/opt/software/spark-2.2.0-bin-hadoop2.7
需要在引用路径的最后添加 $SPARK_HOME/bin:
保存退出
source /etc/profile

spark启动集群:
进入到安装目录找sbin目录进入 /opt/software/spark-2.2.0-bin-hadoop2.7
启动 ./start-all.sh
spark提供webUI界面端和tomcat的端口是一样8080 内部通信7077
http://hadoop01:8080
~~~

#### 集群架构

![](image/20191113162240.png)

#### 配置Job History Server

在运行Spark应用程序的时候，driver会提供一个webUI给出应用程序的运行信息，但是该webUI随着应用程序的完成而关闭端口，也就是 说，Spark应用程序运行完后，将无法查看应用程序的历史记录。Spark history server就是为了应对这种情况而产生的，通过配置，Spark应用程序在运行完应用程序之后，将应用程序的运行信息写入指定目录，而Spark history server可以将这些运行信息装载并以web的方式供用户浏览。 

```shell
1.启动HDFS
start-dfs.sh
创建directory目录
hdfs dfs -mkdir /directory

2.进入到spark安装目录conf目录下
cd /opt/software/spark-2.2.0-bin-hadoop2.7/conf

3.将spark-default.conf.template复制为spark-default.conf
mv spark-defaults.conf.template spark-defaults.conf
vi  spark-defaults.conf在文件的末尾添加
spark.eventLog.enabled           true  开启日志
spark.eventLog.dir               hdfs://hadoop01:8020/directory 存储路径
spark.eventLog.compress          true 是否压缩
参数描述：
spark.eventLog.dir：Application在运行过程中所有的信息均记录在该属性指定的路径下
spark.eventLog.compress 这个参数设置history-server产生的日志文件是否使用压缩，true为使用，false为不使用。这个参数务可以成压缩哦，不然日志文件岁时间积累会过

4.修改spark-env.sh文件，添加如下配置
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=4000 -Dspark.history.retainedApplications=10 -Dspark.history.fs.logDirectory=hdfs://qianfeng01:8020/directory"

spark.history.ui.port=4000  调整WEBUI访问的端口号为4000
spark.history.fs.logDirectory=hdfs://qianfeng01:8020/directory  配置了该属性后，在start-history-server.sh时就无需再显式的指定路径，Spark History Server页面只展示该指定路径下的信息
spark.history.retainedApplications=10   指定保存Application历史记录的个数，如果超过这个值，旧的应用程序信息将被删除，这个是内存中的应用数，而不是页面上显示的应用数。

5.配置完成后分发文件到相应节点
scp -r ./spark-env.sh root@qianfeng02:$PWD
scp -r ./spark-defaults.conf root@qianfeng02:$PWD

ps:最好不要是用IE内核的浏览器不然效果是显示不出来的
   启动的时候是
   start-all.sh       start-history-server.sh 
```

#### Spark高可用

```sh
那就是Master节点存在单点故障，要解决此问题，就要借助zookeeper，并且启动至少两个Master节点来实现高可靠，配置方式比较简单：
Spark集群规划：hadoop01，hadoop04是Master；hadoop02，hadoop03，hadoop04是Worker
安装配置zk集群，并启动zk集群
停止spark所有服务，修改配置文件spark-env.sh，在该配置文件中删掉SPARK_MASTER_IP并添加如下配置
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=hadoop02,hadoop03,hadoop04 -Dspark.deploy.zookeeper.dir=/spark"
分发到hadoop02,hadoop03,hadoop04节点下
1.在hadoop01节点上修改slaves配置文件内容指定worker节点
ps:若是修改了slaves节点那么配置文件也发分发
2.先启动zookeeper集群
3.在hadoop01上执行sbin/start-all.sh脚本，然后在hadoop04上执行sbin/start-master.sh启动第二个Master

ps:若使用spark-shell启动集群需要添加配置
spark-shell --master spark://master01:port1,master02:port2

```

### 2.3 yarn 模式

#### 集群搭建

~~~xml
1，在Hadoop配置下的yarn-site.xml文件中增加以下两项:
这两项判断是否启动一个线程检查每个任务正使用的物理内存量/虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是true 
如果不配置这两个选项，在spark-on-yarn的client模式下，可能会报错,导致程序被终止。
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>
<property> 
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>

2，修改Spark-env.sh 添加：
HADOOP_CONF_DIR=/home/bigdata/hadoop/hadoop-2.7.3/etc/hadoop
YARN_CONF_DIR=/home/bigdata/hadoop/hadoop-2.7.3/etc/hadoop

~~~

#### YARN-Client

在Yarn-client中，Driver运行在Client上，通过ApplicationMaster向RM获取资源。本地Driver负责与所有的executor container进行交互，并将最后的结果汇总。结束掉终端，相当于kill掉这个spark应用。

 

因为Driver在客户端，所以可以通过webUI访问Driver的状态，默认是http://hadoop1:4040访问，而YARN通过http:// hadoop1:8088访问

 

- YARN-client的工作流程步骤为：

![img](image/400827-20171203221258632-1988383277.png)

 

![img](image/400827-20171206174933253-682120820.png)

 

 

- Spark Yarn Client向YARN的ResourceManager申请启动Application Master。同时在SparkContent初始化中将创建DAGScheduler和TASKScheduler等，由于我们选择的是Yarn-Client模式，程序会选择YarnClientClusterScheduler和YarnClientSchedulerBackend
- ResourceManager收到请求后，在集群中选择一个NodeManager，为该应用程序分配第一个Container，要求它在这个Container中启动应用程序的ApplicationMaster，与YARN-Cluster区别的是在该ApplicationMaster不运行SparkContext，只与SparkContext进行联系进行资源的分派
- Client中的SparkContext初始化完毕后，与ApplicationMaster建立通讯，向ResourceManager注册，根据任务信息向ResourceManager申请资源（Container）
- 一旦ApplicationMaster申请到资源（也就是Container）后，便与对应的NodeManager通信，要求它在获得的Container中启动CoarseGrainedExecutorBackend，CoarseGrainedExecutorBackend启动后会向Client中的SparkContext注册并申请Task
- client中的SparkContext分配Task给CoarseGrainedExecutorBackend执行，CoarseGrainedExecutorBackend运行Task并向Driver汇报运行的状态和进度，以让Client随时掌握各个任务的运行状态，从而可以在任务失败时重新启动任务
- 应用程序运行完成后，Client的SparkContext向ResourceManager申请注销并关闭自己

因为是与Client端通信，所以Client不能关闭。

 客户端的Driver将应用提交给Yarn后，Yarn会先后启动ApplicationMaster和executor，另外ApplicationMaster和executor都 是装载在container里运行，container默认的内存是1G，ApplicationMaster分配的内存是driver- memory，executor分配的内存是executor-memory。同时，因为Driver在客户端，所以程序的运行结果可以在客户端显 示，Driver以进程名为SparkSubmit的形式存在。

#### Yarn-Cluster

- 在YARN-Cluster模式中，当用户向YARN中提交一个应用程序后，YARN将分两个阶段运行该应用程序：

1. 第一个阶段是把Spark的Driver作为一个ApplicationMaster在YARN集群中先启动；
2. 第二个阶段是由ApplicationMaster创建应用程序，然后为它向ResourceManager申请资源，并启动Executor来运行Task，同时监控它的整个运行过程，直到运行完成

应用的运行结果不能在客户端显示（可以在history server中查看），所以最好将结果保存在HDFS而非stdout输出，客户端的终端显示的是作为YARN的job的简单运行状况，下图是yarn-cluster模式

![img](./image/400827-20171203220514147-91593573.png)

 

![img](./image/400827-20171206175225316-227997670.png)

 

执行过程： 

- Spark Yarn Client向YARN中提交应用程序，包括ApplicationMaster程序、启动ApplicationMaster的命令、需要在Executor中运行的程序等
- ResourceManager收到请求后，在集群中选择一个NodeManager，为该应用程序分配第一个Container，要求它在这个Container中启动应用程序的ApplicationMaster，其中ApplicationMaster进行SparkContext等的初始化
- ApplicationMaster向ResourceManager注册，这样用户可以直接通过ResourceManage查看应用程序的运行状态，然后它将采用轮询的方式通过RPC协议为各个任务申请资源，并监控它们的运行状态直到运行结束
- 一旦ApplicationMaster申请到资源（也就是Container）后，便与对应的NodeManager通信，要求它在获得的Container中启动CoarseGrainedExecutorBackend，而Executor对象的创建及维护是由CoarseGrainedExecutorBackend负责的，CoarseGrainedExecutorBackend启动后会向ApplicationMaster中的SparkContext注册并申请Task。这一点和Standalone模式一样，只不过SparkContext在Spark Application中初始化时，使用CoarseGrainedSchedulerBackend配合YarnClusterScheduler进行任务的调度，其中YarnClusterScheduler只是对TaskSchedulerImpl的一个简单包装，增加了对Executor的等待逻辑等
- ApplicationMaster中的SparkContext分配Task给CoarseGrainedExecutorBackend执行，CoarseGrainedExecutorBackend运行Task并向ApplicationMaster汇报运行的状态和进度，以让ApplicationMaster随时掌握各个任务的运行状态，从而可以在任务失败时重新启动任务
- 应用程序运行完成后，ApplicationMaster向ResourceManager申请注销并关闭自己

#### YARN-Cluster和YARN-Client的区别

- 理解YARN-Client和YARN-Cluster深层次的区别之前先清楚一个概念：Application Master。在YARN中，每个Application实例都有一个ApplicationMaster进程，它是Application启动的第一个容器。它负责和ResourceManager打交道并请求资源，获取资源之后告诉NodeManager为其启动Container。从深层次的含义讲YARN-Cluster和YARN-Client模式的区别其实就是ApplicationMaster进程的区别
- YARN-Cluster模式下，Driver运行在AM(Application Master)中，它负责向YARN申请资源，并监督作业的运行状况。当用户提交了作业之后，就可以关掉Client，作业会继续在YARN上运行，因而YARN-Cluster模式不适合运行交互类型的作业
- YARN-Client模式下，Application Master仅仅向YARN请求Executor，Client会和请求的Container通信来调度他们工作，也就是说Client不能离开



（1）YarnCluster的Driver是在集群的某一台NM上，但是Yarn-Client就是在RM的机器上； 
（2）而Driver会和Executors进行通信，所以Yarn_cluster在提交App之后可以关闭Client，而Yarn-Client不可以； 
（3）Yarn-Cluster适合生产环境，Yarn-Client适合交互和调试。

 下表是Spark Standalone与Spark On Yarn模式下的比较

![](image/20191113161636.png)



## 第三章 作业提交

### 3.1 spark-submit

提交Spark提供的利用蒙特·卡罗算法求π的例子，其中100这个参数是计算因子

在spark安装目录下bin目录下执行这个例子

```
./spark-submit --class org.apache.spark.examples.SparkPi --master spark://qianfeng01:7077 /usr/local/spark/examples/jars/spark-examples_2.11-2.4.5.jar 100
```

![](./image/2019-09-29_132736.png)

Standalone模式下，集群启动时包括Master与Worker，其中Master负责接收客户端提交的作业，管理Worker

### 3.2 作业执行流程描述

提交任务的节点启动一个driver（client）进程；
dirver进程启动以后，首先是构建sparkcontext，sparkcontext主要包含两部分：DAGScheduler和TaskScheduler
TaskScheduler会寻找Master节点，Master节点接收到Application的注册请求后，通过资源调度算法，在自己的集群的worker上启动Executor进程；启动的executor也会反向注册到TaskScheduler上

Executor进程内部会维护一个线程池，Executor每接收到一个task，都会用TaskRunner封装task，然后从线程池中取出一个线程去执行taskTaskRunner主要包含两种task：ShuffleMapTask和ResultTask，除了最后一个stage是ResultTask外，其他的stage都是ShuffleMapTask，Executor注册到TaskScheduler后，driver进程会对程序进行划分，划分成一个或者多个action；每个action就是一个job；DAGScheduler通过stage划分算法对job进行划分；每个stage创建一个taskset；然后DAGScheduler将taskset提交给TaskScheduler去执行； 


### 3.3 名词解释

1.Standalone模式下存在的角色。

**Client**：客户端进程，负责提交作业到Master。

**Master**：Standalone模式中主控节点，负责接收Client提交的作业，管理Worker，并命令Worker启动Driver和Executor。

**Worker**：Standalone模式中slave节点上的守护进程，负责管理本节点的资源，定期向Master汇报心跳，接收Master的命令，启动Driver和Executor。

**Driver**： 一个Spark作业运行时包括一个Driver进程，也是作业的主进程，负责作业的解析、生成Stage并调度Task到Executor上。包括DAGScheduler，TaskScheduler。

**Executor**：即真正执行作业的地方，一个集群一般包含多个Executor，每个Executor接收Driver的命令Launch Task，一个Executor可以执行一到多个Task。

2.作业相关的名词解释

**Stage**：一个Spark作业一般包含一到多个Stage。

**Task**：一个Stage包含一到多个Task，通过多个Task实现并行运行的功能。

**DAGScheduler**： 实现将Spark作业分解成一到多个Stage，每个Stage根据RDD的Partition个数决定Task的个数，然后生成相应的Task set放到TaskScheduler中。

**TaskScheduler**：实现Task分配到Executor上执行

在web UI界面![图片10](./image/图片10.png)

在启动任务的时候并没有指定分配资源,而是有多少资源就使用了多少资源我们在跑任务的时候是可以指定资源的,可以在指定使用核数和内存资源

```sh
./spark-submit \
> --class org.apache.spark.examples.SparkPi \
> --master spark://hadoop01:7077 \
> --executor-memory 512m \
> --total-executor-cores 2 \
> /opt/software/spark-2.2.0-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.2.0.jar 100

--executor-memory 设置内存 --total-executor-cores 核数
```

## 第四章 SparkShell 

spark-shell是Spark自带的交互式Shell程序，方便用户进行交互式编程，用户可以在该命令行下用Scala编写Spark程序。spark-shell程序一般用作Spark程序测试练习来用。spark-shell属于Spark的特殊应用程序，我们可以在这个特殊的应用程序中提交应用程序

spark-shell启动有两种模式，local模式和cluster模式，分别为

### 4.1 local模式

```
spark-shell 
```

local模式仅在本机启动一个SparkSubmit进程，没有与集群建立联系,虽然进程中有SparkSubmit但是不会被提交到集群红

![图片11](./image/图片11.png)

### 4.2 Cluster模式(集群模式)

```
spark-shell \
--master spark://hadoop01:7077 \
--executor-memory 512m \
--total-executor-cores 1

后两个命令不是必须的 --master这条命令是必须的(除非在jar包中已经指可以不指定,不然就必须指定)
```

### 4.3 退出shell

千万不要ctrl+c spark-shell 正确退出 :quit 千万不要ctrl+c退出 这样是错误的 若使用了ctrl+c退出 使用命令查看监听端口 netstat - apn | grep 4040 在使用kill -9 端口号 杀死即可    

### 4.4  spark2.2shell和spark1.6shell对比 

![图片12](./image/图片12.png)

ps:启动spark-shell若是集群模式,在webUI会有一个一直执行的任务

## 第五章 spark编程

### 5.1  通过IDEA创建Spark工程

ps:工程创建之前步骤省略,在scala中已经讲解,直接默认是创建好工程的

![](./image/20191113173227.png)

#### 对工程中的pom.xml文件配置

```xml
 <!-- 声明公有的属性 -->
<properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.2.0</spark.version>
        <hadoop.version>2.7.1</hadoop.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>
<!-- 声明并引入公有的依赖 -->
    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
        <version>${spark.version}</version>
    </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
    </dependencies>

<!-- 配置构建信息 -->
    <build>
        <!-- 资源文件夹 -->
        <sourceDirectory>src/main/scala</sourceDirectory>
        <!-- 声明并引入构建的插件 -->
        <plugins>
             <!-- 用于编译Scala代码到class -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                        <configuration>
                            <args>
                                <arg>-dependencyfile</arg>
                                <arg>${project.build.directory}/.scala_dependencies</arg>
                            </args>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <!-- 程序打包 -->
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                             <!-- 过滤掉以下文件，不打包 ：解决包重复引用导致的打包错误-->
                            <filters>
                                <filter><artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <!-- 打成可执行的jar包 的主方法入口-->
                                <transformer  implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass></mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>


```

### 5.2 scala实现WordCount

```scala
package Day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//spark版本的wordcount
object SparkWordCount {
  def main(args: Array[String]): Unit = {
       //ps:模板封装成一个方法以后调用方法即可
       //模板代码
       /*
        需要创建SparkConf()对象 相当于MR中配置
        必传参数
        setAppName() 设置任务的名称 不传默认是一个UUID产生名字
        设置运行模式
        不写这个参数可以打包提交集群
        写这个参数设置本地模式
        setMaster() 传入的参数有如下写法
        "local"   --> 本地一个线程来进行任务处理
        "local[数值]" --> 开始相应数值的线程来模拟spark集群运行任务
        "local[*]" --> 开始相应线程数来模拟spark集群运行任务
        两者区别:
        数值类型--> 使用当前数值个数来进行处理
        * -->当前程序有多少空闲线程就用多少空闲线程处理
        */
       val conf = new SparkConf().setAppName("SparkWordCount")
       //创建sparkContext对象
       val sc =  new SparkContext(conf)
    //通过sparkcontext对象就可以处理数据

    //读取文件 参数是一个String类型的字符串 传入的是路径
    val lines: RDD[String] = sc.textFile(args(0))

    //切分数据
     val words: RDD[String] = lines.flatMap(_.split(" "))

    //将每一个单词生成元组 (单词,1)
      val tuples: RDD[(String, Int)] = words.map((_,1))

    //spark中提供一个算子 reduceByKey 相同key 为一组进行求和 计算value
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)

    //对当前这个结果进行排序 sortBy 和scala中sotrBy是不一样的 多了一个参数
    //默认是升序  false就是降序
     val sorted: RDD[(String, Int)] = sumed.sortBy(_._2,false)

    //将数据提交到集群存储 无法返回值
       sorted.saveAsTextFile(args(1))

     //本地模式
    //一定要设置setMaster()
    //可以直接打印
    //println(sorted.collect.toBuffer)
    //这种打印也可以
    //sorted.foreach(println)

    //回收资源停止sc,结束任务
    sc.stop()
  }
}

```

#### 将程序打包成jar包

![图片13](./image/图片13.png)

#### 任务提交

然后将jar包上传到对应的节点上,在spark安装目录下的bin目录下执行

```
./spark-submit \
> --class Day01.SparkWordCount \
> --master spark://hadoop01:7077 \
> --executor-memory 512m \
> --total-executor-cores 2 \
> /root/BigData1815Spark-1.0-SNAPSHOT.jar hdfs://hadoop01:8020/word.txt
hdfs://hadoop01:8020/out2
```

### 5.3 Java实现WordCount

#### 普通版本

~~~java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
/**
 * java版本wordCount
 */
public class JavaWordCount {
    public static void main(String[] args) {
//1.先创建conf对象进行配置主要是设置名称,为了设置运行模式
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
//2.创建context对象
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("dir/file");
//进行切分数据 flatMapFunction是具体实现类
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> splited = Arrays.asList(s.split(" "));
                return splited.iterator();
            }

        });
//将数据生成元组
//第一个泛型是输入的数据类型 后两个参数是输出参数元组的数据
        JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String,
                Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
//聚合
        JavaPairRDD<String, Integer> sumed = tuples.reduceByKey(new Function2<Integer, Integer,
                Integer>() {
            @Override
//第一个Integer是相同key对应的value
//第二个Integer是相同key 对应的value
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
//因为Java api没有提供sortBy算子,此时需要将元组中的数据进行位置调换,然后在排序,排完序在换回
//第一次交换是为了排序
        JavaPairRDD<Integer, String> swaped = sumed.mapToPair(new PairFunction<Tuple2<String,
                Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                return tup.swap();
            }
        });
//排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
//第二次交换是为了最终结果 <单词,数量>
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer,
                String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception
            {
                return tuple2.swap();
            }
        });
        System.out.println(res.collect());
        res.saveAsTextFile("out1");
        jsc.stop();
    }
}
~~~



#### Lambda版本

~~~java

//修改为Lambda表达式
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLamdaWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaLamdaWC").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("dir/file");
		//进行切分数据 flatMapFunction是具体实现类
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将数据生成元组 
        JavaPairRDD<String, Integer> tup = words.mapToPair(word -> new Tuple2<>(word, 1));
        //聚合
        JavaPairRDD<String, Integer> aggred = tup.reduceByKey((v1, v2) -> v1 + v2);
     //因为Java api没有提供sortBy算子,此时需要将元组中的数据进行位置调换,然后在排序,排完序在换回
        JavaPairRDD<Integer, String> swaped = aggred.mapToPair(tuple -> tuple.swap());
      //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
//第二次交换是为了最终结果 <单词,数量>
        JavaPairRDD<String, Integer> res = sorted.mapToPair(tuple -> tuple.swap());

        System.out.println(res.collect());
        res.saveAsTextFile("out1");
        jsc.stop();
    }
}
~~~

