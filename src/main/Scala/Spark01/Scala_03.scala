import scala.util.Random

/**
 * 1.scala模式匹配
 * 2.scala样例类
 * 3.scala偏函数
 * 4.scala函数作为方法参数
 * 5.scala匿名函数
 * 6.scala函数作为方法返回值
 * 7.scala隐式转换和隐式参数
 * 8.scala泛型
 */
object Scala_03 extends App {
  //unapply 与模式匹配
  val person = new Person("Alice", 30)

  person match {
    case Person(name) => println(s"Name: $name") // 提取名字 case Person(name) 调用了 Person.unapply(person)
    case _ => println("Unknown")
  }

  //unapply 与样例类
  val person_01 = Person_01("Alice", 30)

  person_01 match {
    case Person_01(name, age) => println(s"Name: $name, Age: $age")
    case _ => println("Unknown")
  }
}

/**
 * 模式匹配 match case 可以匹配各种情况，比如变量的类型、集合的元素、有值或无值
 *    语法如下：变量 match { case 值 => 代码 }
 *            变量 match { case x:类型 => 代码 }
 */
class MatchCase{
  var a:Int = 0
  val b:Char = 'p'
  val c = 'p'
  var d = 0

  b match {
    case '+' => a = 1
    case '_' => a = -1
    case '*' | 'x' => a = 2
    case c => a = 3  // 可以使用变量
    //守卫模式 case _ 类似Java中的default
    case _ if d<5 => a += d
  }

  //可以对表达式类型进行匹配
  val array = Array("hs",1,2.0,'a')
  val arr = array(Random.nextInt(4))

  arr match {
    // 类型匹配
    case x:Int => println(x)
    case x: String => println(x.toUpperCase)
    case _: Double => println(Int.MaxValue) // 匹配任何类型为Double的对象
    case BigInt => -1                       // 匹配类型为Class的BigInt对象
    case _ => 0

    //泛型匹配
    case m: Map[String,Int] => println()
    case m: Map[_,_] => println() // 匹配通用映射
  }

  // 数组匹配
  val arr1 = Array(1, 1)
  val res = arr1 match {
    case Array(0) => "0"
    //匹配包含0的数组
    case Array(x, y) => s"$x $y"
    // 匹配任何带有两个元素的数组，并将元素绑定到x和y
    case Array(0, _*) => "0..."
    //匹配任何以0开始的数组
    case _ => "something else"
  }
  // 列表匹配
  val list = List(1, 2)
  val res2 = list match {
    case 0 :: Nil => "0"
    case x :: y :: Nil => x + " " + y
    case 0 :: tail => "0 ..."
    case _ => "something else"
  }

  // 元组匹配
  var pair = (1, 2)
  private val tuple = new Tuple2[Int, Int](1, 2)
  val res3 = pair match {
    case (0, _) => "0 ..."
    case (y, 0) => s"$y 0"
    case _ => "neither is 0"
  }

}

/**
 * 样例类：case class  case修饰符可以让Scala编译器自动为类添加一些便捷设定
 *   1.自动生成类的getter和setter访问器：
 *     （1）val 只生成getter
 *     （2）var 同时生成getter和setter
 *   2.自动生成类的伴生对象（单例）
 *     （1）在单例对象中实现apply()方法另外实现了一些其它的方法：toString()、equals()、copy()、hashCode()、unapply()等。特别地unapply()也称为提取器
 */
case class User(var name:String, var age:Int)


class User1{
  // 创建样例类对象
  var people: User = User("jia", 10)
  // 访问对象值
  people.name
}

/**
 * unapply 是一个特殊的方法，通常用于模式匹配和提取器（Extractor）。unapply 的语法 def unapply(obj: T): Option[U]  obj：需要解构的对象。
 *   1.unapply 方法通常定义在伴生对象中，用于解构对象。它的主要功能是：
 *      (1) 从对象中提取出某些值。
 *      (2) 判断对象是否匹配某种模式。
 *   2.unapply 方法的返回值通常是 Option 类型，表示提取是否成功：
 *      (1) 如果匹配成功，返回 Some，包含提取的值。
 *      (2) 如果匹配失败，返回 None。
 */

/**
 * apply 和 unapply 的区别
 *  1. apply方法通常会被称为注入方法(创建对象),在类的伴生对象中做以些初始化的操作
 *  2. apply方法的参数列表不需要和原生类的构造方法参数一致
 *  3. unapply方法通常被提取方法,使用户unapply方法可以提取固定数量的对象或值
 *  4. unapply方法返回的是一个Option类型,因为unapply除了提取值之外,还有一个目的就是判断对象是否创建成功，
 *            若成功则有值,反之没有值,所有才使用Option做为返回值类型
 */
// unapply 与模式匹配
// 从 Person 对象中提取出名字。
class Person(val name: String, val age: Int)

object Person {
  // 定义 unapply 方法
  // Option类型在样例类用来表示可能存在或也可能不存在的值(Option的子类有Some和None)。Some包装了某个值，None表示没有值
  def unapply(person: Person): Option[String] = {
    Some(person.name)
  }
}

// unapply 与样例类
// 样例类 Person 自动生成了 unapply 方法，因此可以直接用于模式匹配。
case class Person_01(name: String, age: Int)

/**
 * 高阶函数：如果一个函数的传入参数为函数或者返回值是函数，则该函数即为高阶函数。
 */
class HighOrderFunction{
  //传入参数为函数:函数是头等公民，和数字一样。不仅可以调用，还可以在变量中存放函数，也可以作为参数传入函数，或者作为函数的返回值
  def applyFunction(f:Int => Int, x: Int):Int = {
    f(x)
  }
  val square = (x:Int) => x*x //定义函数
  val result = applyFunction(square,5) //调用

  //传入参数为匿名函数:不需要给每一个函数命名，就像不必给每个数字命名一样，将函数赋给变量的函数叫做匿名函数
  val number = List(1, 2, 3, 4, 5)
  val square1 = number.map(x => x*x) //使用高阶函数 map，传入匿名函数

  //返回值为函数
  def multiplyBy(factor: Int)= {
    (x: Int) => x * factor // multiplyBy 是一个高阶函数，它返回一个函数 (x: Int) => x * factor。
  }
  val double = multiplyBy(2)
  double(5)

  //函数组合
  def addOne(x: Int): Int = x + 1
  def multiplyByTwo(x: Int): Int = x * 2
  val addOneAndMultiplyByTwo = (x: Int) => multiplyByTwo(addOne(x)) // 组合函数
}

/**
 * 闭包：闭包是一个函数，返回值依赖于声明在函数外部的一个或多个变量。
 *     闭包的核心是捕获外部变量
 * 使用场景：
 *   1 回调函数：将闭包作为回调函数传递给其他函数。、
 *   2 延迟计算：通过闭包延迟计算，直到需要时才执行。
 *   3 函数工厂：通过闭包生成不同的函数。
 *   4 状态封装：通过闭包封装状态，避免使用全局变量。
 */
class Closure{
  // 普通函数
  val multiplier = (i:Int) => i*10

  // 引入一个自由变量 factor，这个变量定义在函数外面。
  // 这样定义的函数变量 multiplier1 成为一个"闭包"，因为它引用到函数外面定义的变量，定义这个函数的过程是将这个自由变量捕获而构成一个封闭的函数。
  var factor = 3
  val multiplier1 = (i:Int) => i * factor
}

/**
 * 柯里化 ：Curring函数
 *  将原来接收两个参数的一个函数，转换为两个函数，第一个函数接收原先的第一个参数，然后返回接收原先第二个参数的第二个函数。
 */
class CurringFunction{
  //非柯里化
  def plainOldSum(x:Int,y:Int) = x + y
  plainOldSum(1,2)

  //柯里化
  def curriedSum(x:Int)(y:Int) = x + y
  curriedSum (1)(2)

  val onePlus = curriedSum(1)_  // 下划线 作为第二个参数的占位符
  onePlus(2)

  //柯里化实现原理
  //1.定义第一个函数
  def first(x:Int) = {
    (y:Int) => x+y
  }
  //2.使用第一个函数生成第二个函数
  val second = first(1)
  second(2)
}

/**
 * 偏函数：被包在花括号内没有match的一组case语句是一个偏函数
 * 偏函数的特点
 *   部分定义：偏函数只对部分输入值定义。
 *   模式匹配：偏函数通常与模式匹配结合使用。
 *   组合操作：偏函数支持 orElse 和 andThen 等组合操作。
 */
class PartialFunctionCase{
  // 1 基本用法
  val isEven: PartialFunction[Int, String] = {
    case x if x % 2 == 0 => s"$x is even"
  }
  // 2 使用偏函数
  println(isEven(4)) // 输出: 4 is even
  println(isEven(3)) // 抛出 MatchError

  // 使用 isDefinedAt 检查输入
  val isEven1: PartialFunction[Int, String] = {
    case x if x % 2 == 0 => s"$x is even"
  }
  println(isEven1.isDefinedAt(4)) // 输出: true
  println(isEven1.isDefinedAt(3)) // 输出: false

  // 3 组合偏函数 偏函数支持 orElse 和 andThen 等组合操作
  val isEven2: PartialFunction[Int, String] = {
    case x if x % 2 == 0 => s"$x is even"
  }
  val isOdd: PartialFunction[Int, String] = {
    case x if x % 2 != 0 => s"$x is odd"
  }
  val checkNumber = isEven2.orElse(isOdd)  //orElse 用于将两个偏函数组合在一起，如果第一个偏函数未定义，则尝试第二个偏函数。
  println(checkNumber(4)) // 输出: 4 is even
  println(checkNumber(3)) // 输出: 3 is odd

  val toUpper: String => String = _.toUpperCase
  val checkNumber1 = isEven.andThen(toUpper) //andThen 用于将一个偏函数的结果传递给另一个函数。checkNumber1 先调用 isEven，然后将结果传递给 toUpper 函数。
  println(checkNumber1(4)) // 输出: 4 IS EVEN

  // 4 偏函数与集合操作 : 偏函数常用于集合操作中，例如 collect 方法。
  val numbers = List(1, 2, 3, 4, 5)
  val isEven3: PartialFunction[Int, String] = {
    case x if x % 2 == 0 => s"$x is even"
  }
  val result = numbers.collect(isEven3)
  println(result) // 输出: List(2 is even, 4 is even)
}

/**
 * 隐式转换 : 使用implicit关键字修饰的带有单个参数的方法
 * 隐式参数 : 使用implicit关键字修饰的变量
 * （implicit 出现于scala 2.1.版本）
 *
 * 作用 ： 隐式的对类的方法进行增强，丰富类库功能
 */
// 定义隐式类，将File转换成定义的隐式类RichFile
implicit class RichFile(val from : File){
  def read : String = Source.fromFile(from.getPath).mkString
}
//使用隐式类做已有类的方法拓展
//val contents : String = new File("xxx").read
//println(contents)

/*
  隐式引用 : 如java.lang 包会被隐式引用到每个scala程序上
  import java.lang._
 */

// 隐式转化 :可以在不改变代码结构的情况下, 将一种类型的值转换为另一种类型 ;
class RichFile1(val from : File){
  def read : String = Source.fromFile(from.getPath).mkString
}
object RichFile1 {
  // 隐式转化方法 :隐式转换函数需要明确指定返回类型
  implicit def file2RichFile(from: File): RichFile = new RichFile(from)
}

/*
   使用隐式转化
  import RichFile1._
  print(new File("xxx"))
 */

/*
    隐式类：提供了一种更简洁的方式来扩展现有类型的功能,隐式类必须定义在其它类、对象或包中, 并且只能有一个参数 .
           在同一作用域内，不能有任何方法、成员或对象与隐式类同名
 */
object Helpers {
  implicit class RichInt(val value: Int) {
    // `n: => Int` 这种写法表示的是一个**懒求值参数**（lazy parameter）。与普通的参数不同，懒求值参数不会立即被计算.如果 `times` 方法没有被调用，那么 `n` 的计算就不会发生
    def times(n: => Int): Int = value * n
  }
  // 调用
  println(2 times 3) //输出6
}

/**
 * 隐式转换函数
 */
class ImplicitToFunction{
  val x = 1
  // 在Scala.Predef对象定义了
  implicit def int2Double(x : Int) :Double = x.toDouble
  val double: Double = x.toDouble
}

/**
 * 隐式参数：在定义函数时，支持在最后一组参数使用implicit，表名此为一组隐式参数，在调用时可以不用给隐式参数传参，编译器会自动寻找一个implicit标记过合适的值作为参数
 */
object Implicit_Parameter{
  // 定义一个特质，方法为抽象方法需实现
  trait Adder[T] {
    def add(x:T,y:T):T
  }

  implicit val sum :Adder[Int] = new Adder[Int] {
    override def add(x:Int , y:Int) : Int = x + y
  }

  // addTest 有隐式参数，当前作用域存在sum（被隐式标记），不用给隐式参数传参编译器可识别
  def addTest(x:Int , y:Int)(implicit adder: Adder[Int]): Unit = {
    adder.add(x,y)
  }

  addTest(1,2)
  addTest(1,2)(sum)
  addTest(1,2)(new Adder[Int] {
    override def add(x: Int, y: Int): Int = x + y
  })
}


/**
 * 泛型 : 是一种允许类、特质和方法参数化的机制。通过泛型，可以编写通用的代码，适用于多种类型，而不需要为每种类型重复编写代码。泛型提高了代码的复用性和类型安全性。
 * [B <: A] upper bounds 上限或上界：B类型的上界是A类型，也就是B类型的父类是A类型
 * [B >: A] lower bounds 下限或下界：B类型的下界是A类型，也就是B类型的子类是A类型
 * [B <% A] view bounds 视界：表示B类型要转换为A类型，需要一个隐式转换函数
 * [B : A] context bounds 上下文界定（用到了隐式参数的语法糖）：需要一个隐式转换的值
 * [-A]：逆变，作为参数类型。是指实现的参数类型是接口定义的参数类型的父类
 * [+B]：协变，作为返回类型。是指返回类型是接口定义返回类型的子类
 */
// 1 泛型基本语法
// (1) 泛型类 : 泛型类是在类定义时使用类型参数的类。类型参数用方括号 [] 表示。
// Box[T] 是一个泛型类，T 是类型参数。 在创建实例时，可以指定具体的类型（如 Int 或 String）
class Box[T](value:T){
  def getValue: T = value
}

/*  使用泛型类
val intBox = new Box[Int](42)
intBox.getValue
 */

// (2) 泛型方法
class Box1{
  //printValue[T] 是一个泛型方法，T 是类型参数。
  def printValue[T](value:T): Unit = {
    println(value)
  }
  // 使用泛型方法
  printValue(42) // 输出: 42
}

// 2 泛型的类型约束
// （1）上界（Upper Bound）:上界用于限制类型参数必须是某个类型的子类型。
class Animal
class Dog extends Animal
class Zoo[T <: Animal](animal: T) { //T <: Animal 表示 T 必须是 Animal 或其子类型。
  def showAnimal: T = animal
}
// 使用上界
//val dogZoo = new Zoo[Dog](new Dog)
//val animalZoo = new Zoo[Animal](new Animal)

//（2）下界（Lower Bound）:下界用于限制类型参数必须是某个类型的父类型。
class Animal1
class Dog1 extends Animal1
class Shelter[T >: Dog1](animal: T) { //T >: Dog 表示 T 必须是 Dog 或其父类型。
  def showAnimal: T = animal
}
// 使用下界
//val animalShelter = new Shelter[Animal1](new Animal1)

// （3）视图界定（View Bound）:视图界定用于限制类型参数必须能够隐式转换为某个类型。
class ViewBound{
  def printLength[T <% String](value: T): Unit = { // T <% String 表示 T 必须能够隐式转换为 String。
    println(value.length)
  }
  // 使用视图界定
  printLength("Hello") // 输出: 5
}

// 3 泛型的协变和逆变
// (1) 协变（Covariant）:协变表示子类型关系在泛型类型中保持不变。用 + 表示。
class Box2[+T](value: T)  //Box[+T] 是协变的，Box[Dog] 是 Box[Animal] 的子类型。
//val animalBox: Box2[Animal] = new Box2[Dog](new Dog)

// （2）逆变（Contravariant）:逆变表示子类型关系在泛型类型中反转。用 - 表示。
class Shelter1[-T] // Shelter[-T] 是逆变的，Shelter[Animal] 是 Shelter[Dog] 的子类型。
//val dogShelter: Shelter1[Dog] = new Shelter1[Animal]

// 4 泛型的类型通配符
class wildcard{
  // （1）上界通配符:上界通配符用于表示类型参数是某个类型的子类型。
  def printAnimals(box: Box[_ <: Animal]): Unit = { //Box[_ <: Animal] 表示 Box 的类型参数是 Animal 的子类型。
    println(box.getValue)
  }
  val dogBox = new Box[Dog](new Dog)
  printAnimals(dogBox)

  // （2）下界通配符:下界通配符用于表示类型参数是某个类型的父类型
  def addAnimal(shelter: Shelter[_ >: Dog1]): Unit = { //Shelter[_ >: Dog] 表示 Shelter 的类型参数是 Dog 的父类型。
    println("Animal added")
  }
  val animalShelter = new Shelter[Animal1](new Animal1)
  addAnimal(animalShelter)
}

/*
下划线使用总结
1、用于类中的var属性，使用默认值。
2、用于高阶函数的第一种用法，表示函数自身。
3、匿名函数化简，用下划线代替变量。
4、用于导包下的所有内容。
5、用于起别名时表示匿名。
6、用于模式匹配表示任意数据。
 */
class UnderLine{
  //  1、用于类中的var属性，使用默认值。
  var name1: String = _

  //  2、用于高阶函数的第一种用法，表示函数自身。
  def sayHi(name: String): Unit = {
    println(s"hi $name")
  }

  val function: String => Unit = sayHi _

  //  3、匿名函数化简，用下划线代替变量。
  val function01: (Int, Int) => Int = (a: Int, b: Int) => a + b
  val function02: (Int, Int) => Int = _ + _

  //  4、用于导包下的所有内容。
  import scala.util.control.Breaks._

  //  5、用于起别名时表示匿名。
  import scala.util.control.{Breaks => _}

  //  6、用于模式匹配表示任意数据。
  10 match {
    case 10 => "10"
    case _ => "other"
  }
}






