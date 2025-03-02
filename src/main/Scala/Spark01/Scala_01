import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//集合
object Scala_03 extends App {
  val demo = new Seq_Tuple

}

//数组
class ArrayDemo{
  /**
   * 不可变数组
   */
  val arr1 = new Array[Int](10)

  arr1(0) = 0
  arr1(1) = 1

  //可以使用伴生对象的apply方法赋值
  val ints: Array[Int] = Array.apply(1, 2, 3, 4, 5)

  //遍历数组
  // 打印
  println(arr1.toList)

  //循环遍历
  for (elem <- arr1) {
    print(elem+" ")
  }

  for (elem <- 0 until arr1.length){
    print(elem+" ")
  }

  //迭代器遍历
  val iterator: Iterator[Int] = arr1.iterator
  while(iterator.hasNext){
    println(iterator.next())
  }

  //匿名函数遍历
  arr1.foreach(e=>println(e))

  //系统函数遍历
  arr1.foreach(println)

  // 增元素形成新的数组 不能删元素 val arr3 = arr1 :-1 （报错）
  val arr2 = arr1 :+1

  /**
   * 可变数组
   */
  val arr3 : ArrayBuffer[Int] = new ArrayBuffer[Int]()
  val arr4 : ArrayBuffer[Int] = ArrayBuffer(1,2,3)

  arr3.append(1)
  arr3.appendAll(Array(1,3,3,5,6))

  arr3.foreach(println)

  // 更新
  arr3.update(2,1)
  arr3(1) = 100

  //删除
  arr3.remove(0)
  arr4.remove(0,2) //从第0位开始，删两位

  /**
   * arr.toArry    返回结果是一个不可变数组，arr本身没变化
   * arr.toBuffer  返回结果是一个可变数组，arr本身没有变化
   */
  private val buffer: mutable.Buffer[Int] = arr1.toBuffer
  private val array: Array[Int] = arr3.toArray

}

class Seq_List{
  /**
   * 不可变list
   */
  val list1: List[Any] = List(1, 2, 3, "asdf", 'c')
  val list2 = List(1,3,4,5,6,7)

  //list1.foreach(println)

  //增加数据（末尾） :+   （开头增加）   ::
  val list01 = list1 :+ "123"
  val list001 = "123" :+ list1
  val list02 = 100 :: list2
  //list01.foreach(print)

  //合并两个集合 一个集合插入另一个集合中  List(1, 2, 3, asdf, c)134567
  val list03 = list1 :: list2

  //list03.foreach(print)

  //合并两个集合一个集合元素遍历插入另一个中 123asdfc134567
  val list04: List[Any] = list2 ::: list1
  val list003 = list1.:::(list2)
  //list04.foreach(print)

  //concat 等价于 :::
  val list05: List[Any] = list1.concat(list2)
  list05.foreach(print)

  /**
   * head 返回列表第一个元素
   * tail 返回一个列表，包含除了第一元素之外的其他元素
   * isEmpty 在列表为空时返回true
   */
  list1.head
  list1.tail
  list1.isEmpty


  //List.reverse 用于将列表的顺序反转

  /**
   * 可变List
   */
  val listBuffer01 = new ListBuffer[Int]()

  val listBuffer02: ListBuffer[Any] = ListBuffer(1, 2, 3, "asdf")

  //添加元素（末尾）
  listBuffer01.append(1)

  //添加元素（开头）
  listBuffer02.prepend(1)
  //删除元素
  listBuffer02.remove(1)

  //更新值
  listBuffer02.update(1,0)
}

class Seq_Set {
  /**
   * 不可变
   */
  //无序不重复
  val set1: Set[Int] = Set(1, 3, 4, 45, 34)
  val set2: Set[Int] = Set(13, 233, 324, 445, 434)
  //set1.foreach(print)
  //print(set1)

  //判断是否有某个元素
  private val bool: Boolean = set1.contains(1)

  //合并
  private val set3: Set[Int] = set1.concat(set2)

  /**
   * 可变  mutable.Set
   */
  val set4: mutable.Set[Int] = mutable.Set(1, 2, 3, 4, 5, 6)

  //增加元素
  println(set4.add(7)) //存在则返回false 不存在则返回true并添加元素
  println(set4)
  set4 += 7

  //删除元素
  set4.remove(1) //存在则返回false 不存在则返回true并删除元素
  set4 -= 1
  println(set4)

  //连接集合
  val set5: Set[Int] = set1 ++ set2
  val set6: Set[Int] = set1.concat(set2)

  // 最大最小值
  set1.max
  set1.min

  // 两集合求交集
  set1.intersect(set2)
  set1.&(set2)
}

class Seq_Map {
  /**
   * 不可变map
   */
  //1. 创建
  val map1: Map[String, Int] = Map("hello" -> 100, "world" -> 200)
  val map2: Map[String, Int] = Map(("a", 1), ("b", 2), ("C", 3))

  //2. 遍历
  map1.foreach(println)
  println(map2)
  val iterator: Iterator[(String, Int)] = map2.iterator

  while (iterator.hasNext){
    println(iterator.next())
  }

  //遍历key和value
  private val keys: Iterable[String] = map1.keys
  keys.foreach(println)

  private val values: Iterable[Int] = map1.values
  values.foreach(println)

  map1.keys.foreach { ele =>
    print("key:" + ele)
    println("value:"+map1(ele))
  }

  // 根据key获得vaue （option）
  private val maybeInt: Option[Int] = map1.get("hello")

  println(maybeInt.getOrElse(1))  // 1为默认值

  /**
   * 可变Map
   */
  private val map3: mutable.Map[Int, Int] = mutable.Map((1, 1), (2, 2), (3, 3))
  private val map4: mutable.Map[Int, Int] = mutable.Map((6, 1), (7, 2), (8, 3))

  // 增、删、改元素
  map3.put(4,4)
  map3 += (5 -> 5)

  map3.remove(4)
  map3 -= 1

  map3.update(3,4)

  /**
   * Map 合并 可以使用++ 或者 Map.++()
   */
  private val map5: mutable.Map[Int, Int] = map3 ++ map4

  private val map6: mutable.Map[Int, Int] = map3.++(map4)

  private val map7: Map[String, Int] = map1.concat(map2)  //不可变map可用
}

class Seq_Tuple {
  /**
   * 元组也是可以理解为一个容器，可以存放各种相同或不同类型的数据。元组中最大只能有22个元素。
   */

  // 声明
  private val tuple: (Int, String, Boolean, Char) = (1, "abc", true, 'a')

  // 也可以用tuple1 ~~ 也可以用tuple22 数字代表参数个数
  private val tuple1 = new Tuple4(1, 2, "asdf", 'a')

  //访问
  println(tuple._1)
  println(tuple.productElement(0))

  //遍历访问
  private val iterator: Iterator[Any] = tuple.productIterator

  while (iterator.hasNext){
    println(iterator.next)
  }

  for(ele <- tuple.productIterator){
    println(ele)
  }
}

