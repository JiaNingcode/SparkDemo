## 今日任务

```
1. 掌握RDD相关概念
2. 掌握RDD算子
```

## 教学重点

```
1. RDD概念梳理
2. RDD算子的详细讲解
```

## 第六章

### 6.1 RDD概念

RDD（Resilient Distributed Dataset）叫做弹性分布式数据集，是Spark中最基本的数据抽象，它代表一个**不可变、可分区**、里面的元素可**并行计算**的集合。RDD具有数据流模型的特点：自动容错、位置感知性调度和可伸缩性。RDD允许用户在执行多个查询时显式地将工作集缓存在内存中，后续的查询能够重用工作集，这极大地提升了查询速度。

在之前学习MR的过程中对数据是没有进行抽象的,而在Spark中对数据进行了抽象,提供一些列处理方法也就是说RDD(弹性分布式数据集)，Spark计算的基石，为用户屏蔽了底层对数据的复杂抽象和处理，为用户提供了一组方便的数据转换与求值方法。

现在开发的过程中都是面向对象的思想,那么我们创建类的时候会对类封装一些属性和方法,那么创建出来的对象就具备着这些属性和方法,类也属于对数据的抽象,而Spark中的RDD就是对操作数据的一个抽象

查看原码可以得知,而且在类中提供了很多方法可以使用

![图片1](./assets/图片1.png)

总结:

在 Spark 中，对数据的所有操作不外乎创建 RDD、转化已有RDD 以及调用 RDD 操作进行求值。每个 RDD 都被分为多个分区，这些分区运行在集群中的不同节点上。RDD 可以包含 Python、Java、Scala 中任意类型的对象， 甚至可以包含用户自定义的对象。RDD具有数据流模型的特点：自动容错、位置感知性调度和可伸缩性。RDD允许用户在执行多个查询时显式地将工作集缓存在内存中，后续的查询能够重用工作集，这极大地提升了查询速度。

### 6.2 RDD做了什么

```scala
sc.textFile(“xx").flatMap(_.split("")).map((_,1)).reduceByKey(_+_).saveAsTextFile(“xx")
```

![图片2](./assets/图片2.png)

总结:

RDD的创建->RDD的转换(转换过程中为了减少数据计算有添加缓存)->RDD的行动(输出数据)

RDD的属性 

RDD原码中提供了说明

![图片3](./assets/图片3.png)

1）一组分片（Partition），即数据集的基本组成单位。对于RDD来说，每个分片都会被一个计算任务处理，并决定并行计算的粒度。用户可以在创建RDD时指定RDD的分片个数，如果没有指定，那么就会采用默认值。默认值就是程序所分配到的CPU Core的数目。

2）一个计算每个分区的函数。Spark中RDD的计算是以分片为单位的，每个RDD都会实现compute函数以达到这个目的。compute函数会对迭代器进行复合，不需要保存每次计算的结果。

3）RDD之间的依赖关系。RDD的每次转换都会生成一个新的RDD，所以RDD之间就会形成类似于流水线一样的前后依赖关系。在部分分区数据丢失时，Spark可以通过这个依赖关系重新计算丢失的分区数据，而不是对RDD的所有分区进行重新计算。

4）一个Partitioner，即RDD的分片函数。当前Spark中实现了两种类型的分片函数，一个是基于哈希的HashPartitioner，另外一个是基于范围的RangePartitioner。只有对于key-value的RDD，才会有Partitioner，非key-value的RDD的Parititioner的值是None。Partitioner函数不但决定了RDD本身的分片数量，也决定了parent RDD Shuffle输出时的分片数量。

5）一个列表，存储存取每个Partition的优先位置（preferred location）。对于一个HDFS文件来说，这个列表保存的就是每个Partition所在的块的位置。按照“移动数据不如移动计算”的理念，Spark在进行任务调度的时候，会尽可能地将计算任务分配到其所要处理数据块的存储位置。

总结(RDD的五大特征):

1.RDD可以看做是一些列partition所组成的

2.RDD之间的依赖关系

3.算子是作用在partition之上的

4.分区器是作用在kv形式的RDD上

5.partition提供的最佳计算位置,利于数据处理的本地化即计算向数据移动而不是移动数据

ps:RDD本身是不存储数据,可以看做RDD本身是一个引用数据

### 6.3 RDD弹性

1) 自动进行内存和磁盘数据存储的切换

​    Spark优先把数据放到内存中，如果内存放不下，就会放到磁盘里面，程序进行自动的存储切换

2) 基于血统的高效容错机制

​    在RDD进行转换和动作的时候，会形成RDD的Lineage依赖链，当某一个RDD失效的时候，可以通过重新计算上游的RDD来重新生成丢失的RDD数据。

3) Task如果失败会自动进行特定次数的重试

​    RDD的计算任务如果运行失败，会自动进行任务的重新计算，默认次数是4次。

4) Stage如果失败会自动进行特定次数的重试

​    如果Job的某个Stage阶段计算失败，框架也会自动进行任务的重新计算，默认次数也是4次。

5) Checkpoint和Persist可主动或被动触发

​    RDD可以通过Persist持久化将RDD缓存到内存或者磁盘，当再次用到该RDD时直接读取就行。也可以将RDD进行检查点，检查点会将数据存储在HDFS中，该RDD的所有父RDD依赖都会被移除。

6) 数据调度弹性

​    Spark把这个JOB执行模型抽象为通用的有向无环图DAG，可以将多Stage的任务串联或并行执行，调度引擎自动处理Stage的失败以及Task的失败。

7) 数据分片的高度弹性

​    可以根据业务的特征，动态调整数据分片的个数，提升整体的应用执行效率。

​    RDD全称叫做弹性分布式数据集(Resilient Distributed Datasets)，它是一种分布式的内存抽象，表示一个只读的记录分区的集合，它只能通过其他RDD转换而创建，为此，RDD支持丰富的转换操作(如map, join, filter, groupBy等)，通过这种转换操作，新的RDD则包含了如何从其他RDDs衍生所必需的信息，所以说RDDs之间是有依赖关系的。基于RDDs之间的依赖，RDDs会形成一个有向无环图DAG，该DAG描述了整个流式计算的流程，实际执行的时候，RDD是通过血缘关系(Lineage)一气呵成的，即使出现数据分区丢失，也可以通过血缘关系重建分区，总结起来，基于RDD的流式计算任务可描述为：从稳定的物理存储(如分布式文件系统)中加载记录，记录被传入由一组确定性操作构成的DAG，然后写回稳定存储。另外RDD还可以将数据集缓存到内存中，使得在多个操作之间可以重用数据集，基于这个特点可以很方便地构建迭代型应用(图计算、机器学习等)或者交互式数据分析应用。可以说Spark最初也就是实现RDD的一个分布式系统，后面通过不断发展壮大成为现在较为完善的大数据生态系统，简单来讲，Spark-RDD的关系类似于Hadoop-MapReduce关系。

总结:

存储的弹性：内存与磁盘的

自动切换容错的弹性：数据丢失可以

自动恢复计算的弹性：计算出错重试机制

分片的弹性：根据需要重新分片

### 6.4 创建RDD的二种方式

1.从集合中创建RDD

```scala
    val conf = new SparkConf().setAppName("Test").setMaster("local")
      val sc = new SparkContext(conf)
      //这两个方法都有第二参数是一个默认值2  分片数量(partition的数量)
      //scala集合通过makeRDD创建RDD,底层实现也是parallelize
      val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
     //scala集合通过parallelize创建RDD
      val rdd2 = sc.parallelize(Array(1,2,3,4,5,6))
```

2.从外部存储创建RDD

```scala
     //从外部存储创建RDD
      val rdd3 = sc.textFile("hdfs://hadoop01:8020/word.txt")
```

## 第七章 RDD编程

RDD支持两种操作:转化操作和行动操作。RDD 的转化操作是返回一个新的 RDD的操作，比如 map()和 filter()，而行动操作则是向驱动器程序返回结果或把结果写入外部系统的操作。比如 count() 和 first()。 

Spark采用惰性计算模式，RDD只有第一次在一个行动操作中用到时，才会真正计算。Spark可以优化整个计算过程。默认情况下，Spark 的 RDD 会在你每次对它们进行行动操作时重新计算。如果想在多个行动操作中重用同一个 RDD，可以使用 RDD.persist() 让 Spark 把这个 RDD 缓存下来。

### 7.1 Transformation算子

RDD中的所有转换都是延迟加载的，也就是说，它们并不会直接计算结果。相反的，它们只是记住这些应用到基础数据集（例如一个文件）上的转换动作。只有当发生一个要求返回结果给Driver的动作时，这些转换才会真正运行。这种设计让Spark更加有效率地运行。

| **转换**                                                 | **含义**                                                     |
| -------------------------------------------------------- | ------------------------------------------------------------ |
| **map**(func)                                            | 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成 |
| **filter**(func)                                         | 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成 |
| **flatMap**(func)                                        | 类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素） |
| **mapPartitions**(func)                                  | 类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U] |
| **mapPartitionsWithIndex**(func)                         | 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Iterator[T]) => Iterator[U] |
| **sample**(withReplacement, fraction, seed)              | 根据fraction指定的比例对数据进行采样，可以选择是否使用随机数进行替换，seed用于指定随机数生成器种子 |
| **union**(otherDataset)                                  | 对源RDD和参数RDD求并集后返回一个新的RDD                      |
| **intersection**(otherDataset)                           | 对源RDD和参数RDD求交集后返回一个新的RDD                      |
| **distinct**([numTasks]))                                | 对源RDD进行去重后返回一个新的RDD                             |
| **groupByKey**([numTasks])                               | 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD        |
| **reduceByKey**(func, [numTasks])                        | 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，与groupByKey类似，reduce任务的个数可以通过第二个可选的参数来设置 |
| **aggregateByKey**(zeroValue)(seqOp, combOp, [numTasks]) | 相同的Key值进行聚合操作，在聚合过程中同样使用了一个中立的初始值zeroValue:中立值,定义返回value的类型，并参与运算seqOp:用来在同一个partition中合并值combOp:用来在不同partiton中合并值 |
| **sortByKey**([ascending], [numTasks])                   | 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD |
| **sortBy**(func,[ascending], [numTasks])                 | 与sortByKey类似，但是更灵活                                  |
| **join**(otherDataset, [numTasks])                       | 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD |
| **cogroup**(otherDataset, [numTasks])                    | 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD |
| **cartesian**(otherDataset)                              | 笛卡尔积                                                     |
| **pipe**(command, [envVars])                             | 将一些shell命令用于Spark中生成新的RDD                        |
| **coalesce**(numPartitions**)**                          | 重新分区                                                     |
| **repartition**(numPartitions)                           | 重新分区                                                     |
| **repartitionAndSortWithinPartitions**(partitioner)      | 重新分区和排序                                               |

### 7.2 Action算子

在RDD上运行计算,并返回结果给Driver或写入文件系统    

| **动作**                                          | **含义**                                                     |
| ------------------------------------------------- | ------------------------------------------------------------ |
| **reduce**(*func*)                                | 通过func函数聚集RDD中的所有元素，这个功能必须是可交换且可并联的 |
| **collect**()                                     | 在驱动程序中，以数组的形式返回数据集的所有元素               |
| **count**()                                       | 返回RDD的元素个数                                            |
| **first**()                                       | 返回RDD的第一个元素（类似于take(1)）                         |
| **take**(*n*)                                     | 返回一个由数据集的前n个元素组成的数组                        |
| **takeSample**(*withReplacement*,*num*, [*seed*]) | 返回一个数组，该数组由从数据集中随机采样的num个元素组成，可以选择是否用随机数替换不足的部分，seed用于指定随机数生成器种子 |
| **takeOrdered**(*n*, *[ordering]*)                | takeOrdered和top类似，只不过以和top相反的顺序返回元素        |
| **saveAsTextFile**(*path*)                        | 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本 |
| **saveAsSequenceFile**(*path*)                    | 将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。 |
| **saveAsObjectFile**(*path*)                      |                                                              |
| **countByKey**()                                  | 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。 |
| **foreach**(*func*)                               | 在数据集的每一个元素上，运行函数func进行更新。               |

### 7.3 简单算子演示

#### map

使用场景：

对RDD的每一个元素进行操作（通过传入函数实现），返回一个新的RDD，该RDD由原RDD中的每个元素

经过function转换后组成

```scala
def map[U](f: (T) ⇒ U)(implicit arg0: ClassTag[U]): RDD[U]
```

![](assets/map.png)

使用：

```scala
scala> val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = a.map(_.length)
b: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[1] at map at <console>:29

scala> val c = a.zip(b)
c: org.apache.spark.rdd.RDD[(String, Int)] = ZippedPartitionsRDD2[2] at zip at <console>:31

scala> c.collect
res0: Array[(String, Int)] = Array((dog,3), (salmon,6), (salmon,6), (rat,3), (elephant,8))

//问题：不使用zip函数怎么解决？
```

#### mapPartitions

```scala
def mapPartitions[U](f: (Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
```

mapPartitions是map的一个变种。map的输入函数应用于RDD中的每个元素，而mapPartitions的输入函数应用于每个分区，也就是把每个分区中的内容作为整体来处理的。

![](assets/mapPartition.png)

使用：

```scala
scala> val rdd = sc.parallelize(List("dog","cat","chick","monkey"))
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:2    
scala> rdd.mapPartitions(iter=>iter.filter(_.length>3))
res0: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at mapPartitions at <console>:30

scala> res0.collect
res1: Array[String] = Array(chick, monkey)                                      
```

#### mapPartitionsWithIndex

```scala
def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
```

map函数同样作用于整个分区，同时可以获取分区的index信息。

使用：

```scala
scala> val x = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[13] at parallelize at <console>:27

scala> def myfunc(index: Int, iter: Iterator[Int]) : Iterator[String] = {
     |   iter.map(x => index + ":" + x)
     | }
myfunc: (index: Int, iter: Iterator[Int])Iterator[String]

scala> x.mapPartitionsWithIndex(myfunc).collect()
res6: Array[String] = Array(0:1, 0:2, 0:3, 1:4, 1:5, 1:6, 2:7, 2:8, 2:9, 2:10)

```

#### glom

场景：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]

```scala
val rdd = sc.parallelize(1 to 20,4)
rdd.glom().collect()
```

#### **flatMap**

```scala
def flatMap[U](f: (T) ⇒ TraversableOnce[U])(implicit arg0: ClassTag[U]): RDD[U]
```

先map再flat

![](assets/flatmap.png)

使用：

```scala
scala> val a = sc.parallelize(1 to 10, 5)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

scala> a.flatMap(1 to _).collect
res1: Array[Int] = Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

```

#### mapValues

mapValues ：针对（Key， Value）型数据中的 Value 进行 Map 操作，而不对 Key 进行处理。

```scala
scala> val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[2] at parallelize at <console>:27

scala> val b = a.map(x => (x.length, x))
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[3] at map at <console>:29

scala> b.mapValues("x" + _ + "x").collect
res2: Array[(Int, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx), (5,xeaglex))

```

#### **filter**

场景：对RDD中的元素进行过滤

该RDD由原RDD中满足函数中的条件的元素组成

```scala
def filter(f: (T) ⇒ Boolean): RDD[T]
```

使用：

```scala
scala> val a = sc.parallelize(1 to 10, 3)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = a.filter(_ % 2 == 0)
b: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[1] at filter at <console>:29

scala> b.collect
res0: Array[Int] = Array(2, 4, 6, 8, 10) 
```

#### keyBy

```
def keyBy[K](f: T => K): RDD[(K, T)]
```

通过在每个元素上应用一个函数生成键值对中的键，最后构成一个键值对元组。

使用：

```scala
scala> val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[35] at parallelize at <console>:27

scala> val b = a.keyBy(_.length)
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[36] at keyBy at <console>:29

scala> b.collect
res9: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
```

#### groupBy(func)

场景：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。

```
val rdd = sc.parallelize(1 to 20)
rdd.groupBy(_%2).collect
```

#### groupByKey

```
def groupByKey(): RDD[(K, Iterable[V])]
val words = Array("one", "two", "two", "three", "three", "three")
val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
val wordCountsWithGroup = wordPairsRDD .groupByKey() .map(t => (t._1, t._2.sum)) .collect()
```

按照key值进行分组

注意1：每次结果可能不同，因为发生shuffle

使用：

```scala
scala> val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[12] at parallelize at <console>:27

scala> val b = a.keyBy(_.length)
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[13] at keyBy at <console>:29

scala> b.groupByKey.collect
res5: Array[(Int, Iterable[String])] = Array((4,CompactBuffer(lion)), (6,CompactBuffer(spider)), (3,CompactBuffer(dog, cat)), (5,CompactBuffer(tiger, eagle)))

```

#### reduceByKey

```
def reduceByKey(func: (V, V) => V): RDD[(K, V)]
def groupByKey(): RDD[(K, Iterable[V])]
val words = Array("one", "two", "two", "three", "three", "three")
val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

val wordCountsWithReduce = wordPairsRDD .reduceByKey(_ + _) .collect() 

```

对元素为KV对的RDD中Key相同的元素的Value进行reduce操作

使用：

```scala
scala> val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = a.map(x => (x.length, x))
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[1] at map at <console>:29

scala> b.reduceByKey(_ + _).collect
res0: Array[(Int, String)] = Array((3,dogcatowlgnuant)) 
```

#### foldByKey

foldByKey merges the values for each key using an associative function and a neutral "zero value".

针对键值对的RDD进行聚合

```scala
scala> val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[7] at parallelize at <console>:27

scala> val b = a.map(x => (x.length, x))
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[8] at map at <console>:29

scala> b.foldByKey("")(_ + _).collect
res8: Array[(Int, String)] = Array((3,dogcatowlgnuant))

scala> b.foldByKey("QQ")(_ + _).collec
```

### 7.4 进阶算子演示

```scala
  val Iter = (index:Int,iter:Iterator[(Int)]) =>{
    iter.map(x => "[partID:"+index + ", value:"+x+"]")
  }
  def func[T](index:Int,iter:Iterator[(T)]): Iterator[String] ={
    iter.map(x => "[partID:"+index+" value:"+x+"]")
  }
  def main(args: Array[String]): Unit = {
    //遍历出集合中每一个元素
    val conf = new SparkConf().setAppName("OOP").setMaster("local")
    val sc = new SparkContext(conf)

    //遍历出集合中每一个元素
    /*
    mapPartitions是对每个分区中的数据进行迭代
    第一个参数是迭代器送对象, 第二个参数表示是否保留父RDD的partition分区信息
    第二个参数的值默认是false一般是不修改(父RDD就需要使用到宽窄依赖的问题)
    (f: Iterator[T] => Iterator[U],preservesPartitioning: Boolean = false)
    ps:第一个_是迭代器对象 第二个_是分区(集合)中数据
    如果RDD数据量不大,建议采用mapPartition算子代替map算子,可以加快数据量的处理数据
    但是如果RDD中数据量过大比如10亿条,不建议使用mapPartitions开进行数据遍历,可能出现oom内存溢出错误
    */
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6),3)
    val rdd2: RDD[Int] = rdd1.mapPartitions(_.map(_*10))
    println(rdd2.collect().toList)

    /*
    mapPartitionsWithIndex 是对rdd中每个分区的遍历出操作
    (f: (Int, Iterator[T]) => Iterator[U],preservesPartitioning: Boolean = false)
    参数是一个柯里化 第二个参数是一个隐式转换
    函数的作用和mapPartitions是类似,不过要提供两个采纳数,第一个参数是分区号
    第二个参数是分区中元素存储到Iterator中(就可以操作这个Iterator)
    第三个参数是否保留符RDD的partitoin
    */
    val rdd3: RDD[String] = rdd1.mapPartitionsWithIndex(Iter)
    println(rdd3.collect().toList)

    //排序
    /*
    sortBykey
    根据key进行排序,但是key必须实现Ordered接口
    可根据传入的true 或 false 决定是升序 还是 降序
    参数默认是true
     */
    val rdd4 = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    val sorted: RDD[(Int, String)] = rdd4.sortByKey()
    println(sorted.collect().toList)
    /*
    sortBy与sortByKey类似,但是更灵活,可以用func先对数据进行处理,
    然后按处理后的数据比较结果排序
    第一个参数:是处理数据的方式
    第二个参数:是排序方式 默认是true 升序
     */
    val rdd5 = sc.parallelize(List(5,6,4,7,3,8,2,9,10))
    val rdd5_1: RDD[Int] = rdd5.sortBy(x => x, true)
    println(rdd5_1.collect().toBuffer)

    //重新分区
    val rdd6 = sc.parallelize(List(1,2,3,4,5,6),3)
    println("初始化分区:"+rdd6.partitions.length)
    /*
    更改分区repartition可以从少量分区改变为多分区因为会发生shuffle
    根据分区数，从新通过网络随机洗牌所有数据。
     */
    val reps1 = rdd6.repartition(5)
    println("通过repartiton调整分区后的分区:"+ reps1.partitions.length)
    /*
    更改分区 不可以少量分区更改为多分区,因为不会发生shuffle
    缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
     */
    val reps2 = rdd6.coalesce(5)
    println("通过coalesce调整分区后的分区:"+ reps2.partitions.length)

    val rdd6_1 = sc.parallelize(List(("e",5),("c",3),("d",4),("c",2),("a",1)),2)
    //可以传入自定分区器, 也可以传入默认分区器 HashPartitioner
    val reps3: RDD[(String, Int)] = rdd6_1.partitionBy(new HashPartitioner(4))
    println(reps3.partitions.length)
      
    /*
    repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，
    repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
    ps: 必须是可排序的二元组 会根据key值排序
        参数可以是系统分区 也可以是自定义分区
   官方建议，如果需要在repartition重分区之后，还要进行排序，建议直接使用repartitionAndSortWithinPartitions算子。
   因为该算子可以一边进行重分区的shuffle操作，一边进行排序。shuffle与sort两个操作同时进行，
   比先shuffle再sort来说，性能可能是要高的
     */
   rdd4.repartitionAndSortWithinPartitions(new HashPartitioner(1)).foreach(println)

   //求和
   /*
   使用指定的reduce函数，将相同key的值聚合到一起
    */
    val rdd7 = sc.parallelize(Array(("tom",1),("jerry" ,3),("kitty",2),("jerry" ,2),("tom",2),("dog",10)))
    val rdd7_1: RDD[(String, Int)] = rdd7.reduceByKey(_ + _)
    println(rdd7_1.collect().toList)
    /*
    aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U)
    在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，
    进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，
    最后将每个分组的value传递给comb函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给comb函数，以此类推），
    将key与计算结果作为一个新的kv对输出。 seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分区中的结果
    zeroValue是初始值(默认值) seqOP局部聚合(分区)  combOp 全局聚合
     */
    val rdd8 = sc.parallelize(List(("cat",2),("cat",5),("pig",10),("dog",3),("dog",4),("cat",4)),2)
    println("-------------------------------------华丽的分割线------------------------------------------")
    println(rdd8.mapPartitionsWithIndex(func).collect.toList)
    /*
    因为第一次给的数值每个分区中是没有相同key所有都是最大值,所有就相当于值都值相加了
    第二次将同一个分区中的key有相同
    首先会根据相同key来进行计算,以cat为例先会和初始值-进行计算比较留下最大值
    然后会的等待第二分区完成计算,然后在进行一个全局的聚合
    */
    val value: RDD[(String, Int)] = rdd8.aggregateByKey(0)(math.max(_,_),_+_)
    println(value.collect.toBuffer)
    /*
    combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C)
    对相同Key，把Value合并成一个集合.
    createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，
                   要么就和之前的某个元素的键相同。如果这是一个新的元素,combineByKey() 
                   会使用一个叫作 createCombiner() 的函数来创建那个键对应的累加器的初始值
    mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 
                 方法将该键的累加器对应的当前值与这个新的值进行合并
    mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。
                    如果有两个或者更多的分区都有对应同一个键的累加器， 
                    就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
     */
    val rdd9 = sc.parallelize(List(("cat",2),("cat",5),("pig",10),("dog",3),("dog",4),("cat",4)),2)
    val rdd9_1: RDD[(String, Int)] = rdd9.combineByKey(x => x, (a:Int, b:Int)=>a+b, (m:Int, n:Int)=> m + n)
    println(rdd9_1.collect().toList)
```

#### 

#### aggregateByKey

```scala
    /*
    aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U)
    在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，
    进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，
    最后将每个分组的value传递给comb函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给comb函数，以此类推），
    将key与计算结果作为一个新的kv对输出。 seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分区中的结果
    zeroValue是初始值(默认值) seqOP局部聚合(分区)  combOp 全局聚合
     */
def aggregateByKeyU(seqOp: (U, V) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): RDD[(K, U)]

def aggregateByKeyU(seqOp: (U, V) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): RDD[(K, U)]

def aggregateByKeyU(seqOp: (U, V) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): RDD[(K, U)]

```

使用：

```scala
val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)

// lets have a look at what is in the partitions
def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
  iter.map(x => "[partID:" +  index + ", val: " + x + "]")
}
pairRDD.mapPartitionsWithIndex(myfunc).collect

res2: Array[String] = Array([partID:0, val: (cat,2)], [partID:0, val: (cat,5)], [partID:0, val: (mouse,4)], [partID:1, val: (cat,12)], [partID:1, val: (dog,12)], [partID:1, val: (mouse,2)])

pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
res3: Array[(String, Int)] = Array((dog,12), (cat,17), (mouse,6))

pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
res4: Array[(String, Int)] = Array((dog,100), (cat,200), (mouse,200))
```

#### combineByKey

（例子）求各科平均成绩

```scala
    /*
    combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C)
    对相同Key，把Value合并成一个集合.
    createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，
                   要么就和之前的某个元素的键相同。如果这是一个新的元素,combineByKey() 
                   会使用一个叫作 createCombiner() 的函数来创建那个键对应的累加器的初始值
    mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 
                 方法将该键的累加器对应的当前值与这个新的值进行合并
    mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。
                    如果有两个或者更多的分区都有对应同一个键的累加器， 
                    就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
     */

scala> sc.setLogLevel("WARN")

scala> val inputrdd = sc.parallelize(Seq(
                        ("maths", 50), ("maths", 60),
                        ("english", 65),
                        ("physics", 66), ("physics", 61), ("physics", 87)), 
                        1)
inputrdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[41] at parallelize at <console>:27

scala> inputrdd.getNumPartitions                      
res55: Int = 1

scala> val reduced = inputrdd.combineByKey(
         (mark) => {
           println(s"Create combiner -> ${mark}")
           (mark, 1)
         },
         (acc: (Int, Int), v) => {
           println(s"""Merge value : (${acc._1} + ${v}, ${acc._2} + 1)""")
           (acc._1 + v, acc._2 + 1)
         },
         (acc1: (Int, Int), acc2: (Int, Int)) => {
           println(s"""Merge Combiner : (${acc1._1} + ${acc2._1}, ${acc1._2} + ${acc2._2})""")
           (acc1._1 + acc2._1, acc1._2 + acc2._2)
         }
     )
reduced: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[42] at combineByKey at <console>:29

scala> reduced.collect()
Create combiner -> 50
Merge value : (50 + 60, 1 + 1)
Create combiner -> 65
Create combiner -> 66
Merge value : (66 + 61, 1 + 1)
Merge value : (127 + 87, 2 + 1)
res56: Array[(String, (Int, Int))] = Array((maths,(110,2)), (physics,(214,3)), (english,(65,1)))

scala> val result = reduced.mapValues(x => x._1 / x._2.toFloat)
result: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[43] at mapValues at <console>:31

scala> result.collect()
res57: Array[(String, Float)] = Array((maths,55.0), (physics,71.333336), (english,65.0))
```

#### sortByKey

```scala
def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P]
```

使用：

```scala
scala> val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = sc.parallelize(1 to a.count.toInt, 2)
b: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:29

scala> val c = a.zip(b)
c: org.apache.spark.rdd.RDD[(String, Int)] = ZippedPartitionsRDD2[2] at zip at <console>:31

scala> c.sortByKey(true).collect
res0: Array[(String, Int)] = Array((ant,5), (cat,2), (dog,1), (gnu,4), (owl,3))

scala> c.sortByKey(false).collect
res1: Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))

```

#### sortBy

```
def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.size)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]


```

使用：

```scala
scala> val y = sc.parallelize(Array(5, 7, 1, 3, 2, 1))
y: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[9] at parallelize at <console>:27

scala> y.sortBy(c => c, true).collect
res2: Array[Int] = Array(1, 1, 2, 3, 5, 7)

scala> y.sortBy(c => c, false).collect
res3: Array[Int] = Array(7, 5, 3, 2, 1, 1)

scala> val z = sc.parallelize(Array(("H", 10), ("A", 26), ("Z", 1), ("L", 5)))
z: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[22] at parallelize at <console>:27

scala> z.sortBy(c => c._1, true).collect
res7: Array[(String, Int)] = Array((A,26), (H,10), (L,5), (Z,1))

```

    sortBy与sortByKey类似,但是更灵活,可以用func先对数据进行处理,
    然后按处理后的数据比较结果排序
    第一个参数:是处理数据的方式
    第二个参数:是排序方式 默认是true 升序
#### **union, ++**

```
def ++(other: RDD[T]): RDD[T]
def union(other: RDD[T]): RDD[T]



```

合并同一数据类型元素，但不去重。

使用

```scala
scala> val a = sc.parallelize(1 to 3,1)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = sc.parallelize(5 to 7,1)
b: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:27

scala> (a++b).collect
res0: Array[Int] = Array(1, 2, 3, 5, 6, 7)    
```

#### intersection

```
def intersection(other: RDD[T], numPartitions: Int): RDD[T]
```

返回两个差集，不含重复元素。

```scala
scala> val x = sc.parallelize(1 to 20)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

scala> val y = sc.parallelize(10 to 30)
y: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[4] at parallelize at <console>:27

scala> val z = x.intersection(y)
z: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[10] at intersection at <console>:31

scala> 

scala> z.collect
res1: Array[Int] = Array(16, 14, 12, 18, 20, 10, 13, 19, 15, 11, 17)

```

#### distinct

```scala
def distinct(): RDD[T]
def distinct(numPartitions: Int): RDD[T]
```

去重元素之后的RDD

```scala
scala> val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
c: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> c.distinct.collect
res0: Array[String] = Array(Dog, Cat, Gnu, Rat)                                 

scala> val a = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at parallelize at <console>:27

scala> a.distinct(2).partitions.length
res3: Int = 2

scala> a.distinct(3).partitions.length
res4: Int = 3
```



#### join

要求原RDD为PairRDD，将原RDD和目标RDD按Key合并 ,最后返回 RDD[(K， (V， W))]。

join算子（join，leftOuterJoin，rightOuterJoin）

1)只能通过PairRDD使用；

2) join算子操作的Tuple2<Object1, Object2>类型中，Object1是连接键;

join(otherDataset, [numTasks])是连接操作，将输入数据集(K,V)和另外一个数据集(K,W)进行Join， 得到(K, (V,W))；该操作是对于相同K的V和W集合进行笛卡尔积 操作，也即V和W的所有组合；

```
def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
```

使用：

```scala
scala> val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[28] at parallelize at <console>:27

scala> val b = a.keyBy(_.length)
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[29] at keyBy at <console>:29

scala> val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
c: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[30] at parallelize at <console>:27

scala> val d = c.keyBy(_.length)
d: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[31] at keyBy at <console>:29

scala> b.join(d).collect
res8: Array[(Int, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))

```

#### repartition

```
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
```

This function changes the number of partitions to the number specified by the numPartitions parameter 

```scala
scala> val rdd = sc.parallelize(List(1, 2, 10, 4, 5, 2, 1, 1, 1), 3)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[37] at parallelize at <console>:27

scala> rdd.partitions.length
res10: Int = 3

scala> val rdd2 = rdd.repartition(5)
rdd2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[41] at repartition at <console>:29

scala> rdd2.partitions.length
res11: Int = 5

```

#### cogroup

可以对多达3个RDD根据key进行分组，将每个Key相同的元素分别聚集为一个集合

返回一个新的RDD，格式为 (K, (Iterable[V], Iterable[W]))，将两个PairRDD按Key合并，返回各 自的迭代 

![](assets/coGroup.png)

```scala
scala> val a = sc.parallelize(List(1, 2, 1, 3), 1)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = a.map((_, "b"))
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[1] at map at <console>:29

scala> val c = a.map((_, "c"))
c: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[2] at map at <console>:29

scala> b.cogroup(c).collect
res0: Array[(Int, (Iterable[String], Iterable[String]))] = Array((1,(CompactBuffer(b, b),CompactBuffer(c, c))), (3,(CompactBuffer(b),CompactBuffer(c))), (2,(CompactBuffer(b),CompactBuffer(c))))

```

#### sample

使用场景：数据倾斜发现

```scala
def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
```

返回抽样得到的子集。

withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳

使用：

```scala
scala> val a = sc.parallelize(1 to 10000, 3)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> a.sample(false, 0.1, 0).count
res0: Long = 1032   
```

### 7.5 算子比较

#### 比较**map flatMap mapPartitions mapPartitionsWithIndex**

Spark中，最基本的原则，就是每个task处理一个RDD的partition。

MapPartitions操作的优点：

如果是普通的map，比如一个partition中有1万条数据；ok，那么你的function要执行和计算1万次。

但是，使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有
的partition数据。只要执行一次就可以了，性能比较高。

MapPartitions的缺点：可能会OOM。

如果是普通的map操作，一次function的执行就处理一条数据；那么如果内存不够用的情况下，
比如处理了1千条数据了，那么这个时候内存不够了，那么就可以将已经处理完的1千条数据从
内存里面垃圾回收掉，或者用其他方法，腾出空间来吧。

mapPartition()：每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM。

所以说普通的map操作通常不会导致内存的OOM异常。

在项目中，自己先去估算一下RDD的数据量，以及每个partition的量，还有自己分配给每个executor
的内存资源。看看一下子内存容纳所有的partition数据，行不行。如果行，可以试一下，能跑通就好。
性能肯定是有提升的。

```scala
//map和partition的区别：
scala> val rdd2 = rdd1.mapPartitions(_.map(_*10))
rdd2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[1] ...

scala> rdd2.collect
res1: Array[Int] = Array(10, 20, 30, 40, 50, 60, 70)

scala> rdd1.map(_ * 10).collect
res3: Array[Int] = Array(10, 20, 30, 40, 50, 60, 70)

介绍mapPartition和map的区别，引出下面的内容：

mapPartitionsWithIndex
val func = (index: Int, iter: Iterator[(Int)]) => {
  iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
}
val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 2)
rdd1.mapPartitionsWithIndex(func).collect 
```

#### 比较reduceByKey和groupByKey，aggregateByKey

reduceByKey(func, numPartitions=None)

Merge the values for each key using an associative reduce function. This will also perform the merging**locally on each mapper**before sending results to a reducer, similarly to a “combiner” in MapReduce. Output will be hash-partitioned with numPartitions partitions, or the default parallelism level if numPartitions is not specified.

也就是，reduceByKey用于对每个key对应的多个value进行merge操作，最重要的是它能够在本地先进行merge操作，并且merge操作可以通过函数自定义。

**groupByKey(numPartitions=None)**

Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with numPartitions partitions.** Note**: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will provide much better performance.

也就是，groupByKey也是对每个key进行操作，但只生成一个sequence。需要特别注意“Note”中的话，它告诉我们：如果需要对sequence进行aggregation操作（注意，groupByKey本身不能自定义操作函数），那么，选择reduceByKey/aggregateByKey更好。这是因为groupByKey不能自定义函数，我们需要先用groupByKey生成RDD，然后才能对此RDD通过map进行自定义函数操作。

```
val words = Array("one", "two", "two", "three", "three", "three")  
val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))  
val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _)  
val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))
```

reduceByKey  先在分区上进行合并，然后shuffle，最终得到一个结果。

#### 比较coalesce, repartition

这两个算子都是用来调整分区个数的，其中repartition等价于 coalesce(numPartitions, shuffle = true).

```scala
def coalesce ( numPartitions : Int , shuffle : Boolean = false ): RDD [T]
def repartition ( numPartitions : Int ): RDD [T]
```

使用

```scala
scala> val rdd1 = sc.parallelize(1 to 10,10)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27    
scala> rdd1.partitions.length
res0: Int = 10

scala> val rdd2 = rdd1.coalesce(3,false)
rdd2: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[1] at coalesce at <console>:29

scala> rdd2.partitions.length
res1: Int = 3


```

```SCALA
repartition（重新分配分区）, coalesce（（合并）重新分配分区并设置是否shuffle）,
	partitionBy(根据partitioner函数生成新的ShuffleRDD，将原RDD重新分区)
val rdd1 = sc.parallelize(1 to 10, 10)
rdd1.repartition(5) --分区调整为5个
rdd1.partitions.length =5
coalesce：调整分区数量，参数一:要合并成几个分区，参数二：是否shuffle，false不会shuffle
val rdd2 = rdd1.coalesce(2, false)
val rdd1 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)), 3)
var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
rdd2.partitions.length
```

 

