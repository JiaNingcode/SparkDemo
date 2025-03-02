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

### 7.6 Action算子和其他算子

#### **reduce**

```
def reduce(f: (T, T) => T): T


```

通过传入的函数进行聚合，先分区内聚合，再分区间聚合。

使用：

```scala
scala> val a = sc.parallelize(1 to 100,2)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[49] at parallelize at <console>:27

scala> a.reduce(_+_)
res14: Int = 5050
```

#### fold

fold和reduce的原理相同，但是与reduce不同，相当于每个reduce时，迭代器取的第一个元素是zeroValue。

```
val a = sc.parallelize(List(1,2,3), 3)
a.fold(0)(_ + _)
res59: Int = 6


```

#### **aggregate**

参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)

作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致。



```scala
val z = sc.parallelize(List(1,2,3,4,5,6), 2)

// lets first print out the contents of the RDD with partition labels
def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
  iter.map(x => "[partID:" +  index + ", val: " + x + "]")
}

z.mapPartitionsWithIndex(myfunc).collect
res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1, val: 4], [partID:1, val: 5], [partID:1, val: 6])

z.aggregate(0)(math.max(_, _), _ + _)
res40: Int = 9

// This example returns 16 since the initial value is 5
// reduce of partition 0 will be max(5, 1, 2, 3) = 5
// reduce of partition 1 will be max(5, 4, 5, 6) = 6
// final reduce across partitions will be 5 + 5 + 6 = 16
// note the final reduce include the initial value
z.aggregate(5)(math.max(_, _), _ + _)
res29: Int = 16


val z = sc.parallelize(List("a","b","c","d","e","f"),2)

//lets first print out the contents of the RDD with partition labels
def myfunc(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
  iter.map(x => "[partID:" +  index + ", val: " + x + "]")
}

z.mapPartitionsWithIndex(myfunc).collect
res31: Array[String] = Array([partID:0, val: a], [partID:0, val: b], [partID:0, val: c], [partID:1, val: d], [partID:1, val: e], [partID:1, val: f])

z.aggregate("")(_ + _, _+_)
res115: String = abcdef

// See here how the initial value "x" is applied three times.
//  - once for each partition
//  - once when combining all the partitions in the second reduce function.
z.aggregate("x")(_ + _, _+_)
res116: String = xxdefxabc

// Below are some more advanced examples. Some are quite tricky to work out.

val z = sc.parallelize(List("12","23","345","4567"),2)
z.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
res141: String = 42

z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
res142: String = 11

val z = sc.parallelize(List("12","23","345",""),2)
z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
res143: String = 10
```

```scala
第一个参数是分区里的每个元素相加，第二个参数是每个分区的结果再相加
rdd1.aggregate(0)(_+_, _+_)
需求：把每个分区的最大值取出来，再把各分区最大值相加
rdd1.aggregate(0)(math.max(_, _), _+_)
再看初始值设为10的结果
rdd1.aggregate(10)(math.max(_, _), _+_)
再看初始值设为2的结果
rdd1.aggregate(2)(math.max(_, _), _+_)

def func1(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
}
val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 2)
rdd1.mapPartitionsWithIndex(func1).collect
rdd1.aggregate(0)(math.max(_, _), _ + _)
rdd1.aggregate(5)(math.max(_, _), _ + _)


val rdd2 = sc.parallelize(List("a","b","c","d","e","f"),2)
def func2(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
  iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
}
rdd2.mapPartitionsWithIndex(func2).collect  --查看每个分区的元素
rdd2.aggregate("")(_ + _, _ + _)
查看初始值被应用了几次
rdd2.aggregate("=")(_ + _, _ + _)
如果设了三个分区，初始值被应用了几次？

val rdd3 = sc.parallelize(List("12","23","345","4567"),2)
rdd3.mapPartitionsWithIndex(func2).collect  --查看每个分区的元素
每次返回的值不一样，因为executor有时返回的慢，有时返回的快一些
rdd3.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)

val rdd4 = sc.parallelize(List("12","23","345",""),2)
rdd4.mapPartitionsWithIndex(func2).collect  --查看每个分区的元素
为什么是01或10? 关键点："".length是"0",下次比较最小length就是1了
rdd4.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)

val rdd5 = sc.parallelize(List("12","23","","345"),2)
rdd5.mapPartitionsWithIndex(func2).collect
rdd5.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
```



#### **collect**

collect 将分布式的 RDD 返回为一个单机的 scala Array 数组。在这个数组上运用 scala 的函数式操作。通过函数操作，将结果返回到 Driver 程序所在的节点，以数组形式存储。

使用：

```scala
scala> val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
c: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[50] at parallelize at <console>:27

scala> c.collect
res15: Array[String] = Array(Gnu, Cat, Rat, Dog, Gnu, Rat)
```

#### collectAsMap

类似collect，将键值对以map的形式搜集到driver端。

```scala
scala> val a = sc.parallelize(List(1, 2, 1, 3), 1)
a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> val b = a.zip(a)
b: org.apache.spark.rdd.RDD[(Int, Int)] = ZippedPartitionsRDD2[1] at zip at <console>:29

scala> b.collectAsMap
res0: scala.collection.Map[Int,Int] = Map(2 -> 2, 1 -> 1, 3 -> 3)

```

#### **count**

返回RDD的元素个数  

```scala
scala> val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
c: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[51] at parallelize at <console>:27

scala> c.count
res16: Long = 4

```

#### countByKey()

针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。

```
val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
 rdd.countByKey


```

#### take(n)

返回一个由数据集的前n个元素组成的数组

```scala
val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
b.take(2)
res18: Array[String] = Array(dog, cat)

val b = sc.parallelize(1 to 10000, 5000)
b.take(100)
res6: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100)
```

#### takeSample*(*withReplacement*,*num*, [*seed])

返回一个数组，该数组由从数据集中随机采样的num个元素组成，可以选择是否用随机数替换不足的部分，seed用于指定随机数生成器种子

```scala
scala> val x = sc.parallelize(1 to 1000, 3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:27

scala> x.takeSample(true, 100, 1)
res0: Array[Int] = Array(176, 110, 806, 214, 977, 460, 274, 656, 977, 667, 863, 283, 575, 864, 90, 316, 923, 953, 500, 572, 421, 745, 199, 738, 829, 31, 604, 29, 439, 351, 820, 848, 675, 974, 210, 941, 764, 489, 586, 354, 160, 900, 7, 517, 485, 173, 188, 685, 416, 229, 938, 466, 496, 901, 291, 538, 223, 781, 39, 546, 297, 723, 452, 749, 737, 515, 868, 425, 197, 912, 487, 562, 330, 276, 363, 316, 457, 29, 584, 544, 987, 501, 565, 299, 823, 481, 555, 739, 768, 493, 889, 59, 102, 579, 475, 800, 445, 172, 189, 211)

scala> val x = sc.parallelize(1 to 10, 3)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at parallelize at <console>:27

scala> x.takeSample(true, 20, 1)
res1: Array[Int] = Array(7, 10, 9, 3, 7, 10, 7, 3, 1, 2, 10, 4, 10, 5, 10, 9, 2, 3, 7, 9)
```

#### **takeOrdered**(*n*, *[ordering]*)

takeOrdered和top类似，只不过以和top相反的顺序返回元素

```scala
val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
b.takeOrdered(2)
res19: Array[String] = Array(ape, cat)
```

#### top

top可返回最大的k个元素。 函数定义如下。top（num：Int）（implicit ord：Ordering[T]）：Array[T]
相近函数说明如下。top返回最大的k个元素。

```scala
val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
b.top(3)
```

#### first

返回RDD的第一个元素（类似于take(1)）

```scala
val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
c.first
res1: String = Gnu
```

#### **countByKey**()

  针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。

```scala
scala> val c = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
c: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[4] at parallelize at <console>:27

scala> c.countByKey
res2: scala.collection.Map[Int,Long] = Map(3 -> 3, 5 -> 1)   
```

#### **foreach**(*func*)

在数据集的每一个元素上，运行函数func进行更新

```scala
scala> val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
c: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[7] at parallelize at <console>:27

scala> c.foreach(x => println(x + "s are yummy"))
cats are yummy
dogs are yummy
tigers are yummy
lions are yummy
gnus are yummy
crocodiles are yummy
ants are yummy
whales are yummy
dolphins are yummy
spiders are yummy
```

**注意：**

**1.foreach是作用在每个分区，结果输出到分区；**

**2.由于并行的原因，每个节点上打印的结果每次运行可能会不同。**

#### saveAsTextFile*(*path)

将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本

```scala
val a = sc.parallelize(1 to 10000, 3)
a.saveAsTextFile("mydata_a")
```

##  

```scala
 val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /* Action 算子*/
    //集合函数
    val rdd1 = sc.parallelize(List(2,1,3,6,5),2)
    val rdd1_1 = rdd1.reduce(_+_)
    println(rdd1_1)
    //以数组的形式返回数据集的所有元素
    println(rdd1.collect().toBuffer)
    //返回RDD的元素个数
    println(rdd1.count())
    //取出对应数量的值 默认降序, 若输入0 会返回一个空数组
    println(rdd1.top(3).toBuffer)
    //顺序取出对应数量的值
    println(rdd1.take(3).toBuffer)
    //顺序取出对应数量的值 默认生序
    println(rdd1.takeOrdered(3).toBuffer)
    //获取第一个值 等价于 take(1)
    println(rdd1.first())
    //将处理过后的数据写成文件(存储在HDFS或本地文件系统)
    //rdd1.saveAsTextFile("dir/file1")
    //统计key的个数并生成map k是key名 v是key的个数
    val rdd2 = sc.parallelize(List(("key1",2),("key2",1),("key3",3),("key4",6),("key5",5)),2)
    val rdd2_1: collection.Map[String, Long] = rdd2.countByKey()
    println(rdd2_1)
    //遍历数据
    rdd1.foreach(x => println(x))

    /*其他算子*/
    //统计value的个数 但是会将集合中的一个元素看做是一个vluae
    val value: collection.Map[(String, Int), Long] = rdd2.countByValue
    println(value)
    //filterByRange:对RDD中的元素进行过滤,返回指定范围内的数据
    val rdd3 = sc.parallelize(List(("e",5),("c",3),("d",4),("c",2),("a",1)))
    val rdd3_1: RDD[(String, Int)] = rdd3.filterByRange("c","e")//包括开始和结束的
    println(rdd3_1.collect.toList)
    //flatMapValues对参数进行扁平化操作,是value的值
    val rdd3_2 = sc.parallelize(List(("a","1 2"),("b","3 4")))
    println( rdd3_2.flatMapValues(_.split(" ")).collect.toList)
    //foreachPartition 循环的是分区数据
    // foreachPartiton一般应用于数据的持久化,存入数据库,可以进行分区的数据存储
    val rdd4 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)
    rdd4.foreachPartition(x => println(x.reduce(_+_)))
    //keyBy 以传入的函数返回值作为key ,RDD中的元素为value 新的元组
    val rdd5 = sc.parallelize(List("dog","cat","pig","wolf","bee"),3)
    val rdd5_1: RDD[(Int, String)] = rdd5.keyBy(_.length)
    println(rdd5_1.collect.toList)
    //keys获取所有的key  values 获取所有的values
    println(rdd5_1.keys.collect.toList)
    println(rdd5_1.values.collect.toList)
    //collectAsMap  将需要的二元组转换成Map
    val map: collection.Map[String, Int] = rdd2.collectAsMap()
    println(map)
```

### 7.7 TextFile分区问题

```scala
val rdd1 = sc.parallelize(List(2,3,4,1,7,5,6,9,8))
获取分区的个数:rdd1.partitions.length,在spark-shell中没有指定分区的个数获取的是默认分区数,除了这个外parallelize方法可以使用,指定几个分区就会有几个分区出现

val rdd1 = sc.textFile("hdfs://hadoop02:8020/word.txt",3).flatMap _.split('')).map((_,1)).reduceByKey(_+_)
textFile这个方法是有默认值就是2 除非改变loacl中的即默认值这个只要这个默认值小于2的话会使用小于默认的值
```

![图片4](assets/图片4.png)

这个默认属性是有值的defaultMinPartitions

![图片5](assets/图片5.png)

![图片6](assets/图片6.png)

![图片7](assets/图片7.png)

![图片8](assets/图片8.png)

如果在textfile中传入了分区数,那么这个分区数可能相同也可能不同需要看底层计算!

![图片9](assets/图片9.png)

![图片10](assets/图片10.png)

![图片11](assets/图片11.png)

![图片12](assets/图片12.png)

![图片13](assets/图片13.png)

![图片14](assets/图片14.png)

![图片15](assets/图片15.png)

下面就是分片了,这个就是为什么textfile传入的参数和实际输出的分区可能不符合的原因

总结:

在textFile中没有指定分区的情况下都是默认大小2,除非指定小于2的值

若在textFile中指定了分区,那么切分文件工作，实际上是计算出多少切分大小即多少切分一下，然后将文件按照这个大小切分成多份，最后partition数就是切分文件的个数











 