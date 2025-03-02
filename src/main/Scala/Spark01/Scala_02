import scala.beans.BeanProperty

/**
 * 方法和函数、类和对象、特质、抽象类、继承、枚举
 */
object Scala_02 extends App {
  val jia: Unit = Person6("jia", 28)

}

/**
 * 方法 :
 *    1、方法的返回值类型可以不写编译器会推断，但是递归函数必须指定返回值类型
 *    2、方法返回值默认方法体最后一行，不推荐使用return（使用会导致类型推断失效）
 *    3、方法没有返回值时，返回值为Unit
 */
class Function {
  // 定义方法：（基本格式:def 方法名称(参数列表) : 返回值类型 = 方法体 ）
  def add(x:Int, y:Int) : Int = x + y
  add(1,2)

  //可以缩写为
  def add1(x: Int, y: Int) = x + y
  def add2(x: Int, y: Int) {x + y} //没有返回值，要用大括号

  // 多参数列表的方法
  def add3(x:Int, y:Int)(z:Int) = (x+y)*z

  //无参方法
  def name:Unit = println("jianing")
  name

  def name1():String = System.getProperty("user.name") //System.getProperty 是 Java 中的一个方法，用于获取系统属性或 JVM 参数的值  "user.name"	当前用户的名称
  name1()

  //方法体（多行代码表达式）
  def getMax(a:Int, b:Int) : Unit = {
    if (a > b) {
      println(a)
    } else {
      print(b)
    }
  }

  //java 不支持带有默认值的方法，scala可以通过重载实现类似效果
  def method1(a:Int=1, b:Int, c:Int=3) = println(a+b+c)
  method1(b=2)  //不可以直接吗，method1(2)
  method1(1,2)  //等价于method1(a=1,b=2)

  //可变参数
  var sum: Int = _
  // 可变参数一定是参数列表最后一个参数
  def method2(a: Int, b: Int*) = {
    for (i <- b) {
      sum = sum + a + i
    }
    sum //方法返回值在方法最后一行
  }

  println(method2(1, 2, 3, 4, 5, 6))

  // 函数内部，重复参数的类型是声明参数类型的数组，如果给可变参数传数组会报错“type mismatch”，可以使用  ~ 数组名:_* ~的方式传参
  var arr: Array[Int] = Array(1,2,3,4,5)
  method2(1,arr:_*)
}

/**
 * 函数 : 带有参数的表达式
 */
class Method{
  // 函数定义: val 变量 = (函数参数列表) => 函数体
  val f1 = (a:Int,b:Int) => a+b
  val f2 = (_:Int) + (_:Int)
  val f3 : (Int,Int)  => Int = (_ + _)  // 注意变量名后是 “:”  ,（_ + _） 括号可以不加

  val f4 : ((Int,Int)=>Int) = {(x,y)=>x+y}
  val f5 : (Int,Int) => Int = (x,y) => x+y

  val f6 = new Method{
    def apply(x:Int,y:Int):Int = if (x<y) y else x
  }

  //匿名函数
  (x: Int) => x+1
  //可以定义不带参数的函数
  var userDir = () => System.getProperty("user.dir")
  print(userDir)

  //递归函数
  val factorial : Int=>Int= n =>{
    if(n<1)
      1
    else
      n * factorial(n-1)
  }

  //无参函数
  val a = () => 42

  //方法转换为函数 用下划线
  def add(a:Int,b:Int) =a +b
  val addFunction = add _
  private val function: (Int, Int) => Int = add _

}

/**
 * 定义类
 */
class Student {
  // var修饰的变量，默认同时有公开的getter方法和setter方法
  var name = "jianing"

  // _表示占位符，编译器会根据变量的具体类型赋予相应的初始值，使用占位符变量类型必须指定
  var nickName:String = _

  // 如果赋值为null，则一定要加类型，不加类型，则该属性的类型为null类型
  var address = null // address 类型为NULL类型

  var address1 : String = null // address1 为String类型，默认值为null

  // val修饰的变量是只读属性，只带getter方法但没有setter方法，val修饰变量不能使用占位符
  val age = 10

  // 类私有字段，有私有的getter方法和setter方法，只能在类的内部使用，伴生对象也可以使用
  private var hobby: String = "旅游"

  //protected[this]：在当前子类对象中访问父类的成员，无法通过其他子类对象访问父类的成员
  //private[this]：相对于来说更加严格一些，仅仅可以被同一个类的同一个对象访问
  private[this] val cardId = "123"
}

/**
 * 自定义属性的get和set方法（手动创建的变量getter和setter方法需要遵循以下原则）
 *
 * 1. 将成员变量定义为私有
 * 2. 字段属性名以“_”为前缀，eg： _x
 * 3. getter方法定义为：def x = _x
 * 4. setter方法定义时，方法名为属性名去掉前缀，并加上后缀，后缀“x_=” (_=之间不能有空格)
 */
class Point_1 {
  private var _x:Int = _
  private var _y = 0
  private val bound = 100

  //自定义getter方法
  def x = _x
  //自定义setter方法
  def x_= (newValue:Int) = {
    _x = newValue
  }
}

/**
 * @BeanProperty时，getter和setter方法会自动生成。(生成4中方法)
 * 1. name: String
 * 2. name_= (newValue: String): Unit
 * 3. getName(): String
 * 4. setName (newValue: String): Unit
 */
class Point_2 {
  @BeanProperty var name:String = _
}

/**
 * 构造方法：（如果没有显示定义构造函数，scala会提供默认构造）
 *
 * 辅助构造：辅助构造器是类中额外定义的构造器，用于支持多种初始化方式。每个辅助构造器必须直接或间接调用主构造器。
 */

//主构造：主构造器是类定义的一部分，它的参数直接放在类名后面

/**
主构造器的参数可以带有 val、var 或不带修饰符，具体区别如下：

修饰符	  | 是否生成字段	|是否可读	 |是否可写	  |外部访问性
--------+-------------+--------+----------+---------
val	    | 生成字段	    | 是	   |  否	    |可读，不可写
--------+-------------+--------+----------+---------
var	    | 生成字段	    | 是	   |  是	    |可读，可写
--------+-------------+--------+----------+---------
无修饰符	| 不生成字段	  | 否	   |  否	    |不可访问

 */
/*
  参数带val: 1.被val修饰的参数会自动生成一个不可变字段和一个公共的getter方法
            2.对象被创建后不能修改，外部代码只能访问
 */
class Person1(val name: String, val age: Int)
//等价于
class Person11(name1: String, age1: Int) {
  val name: String = name1
  val age: Int = age1
}

/*
  参数带var: 1.被var修饰的参数会自动生成一个可变的字段和一个公告的getter 和 setter 方法
            2.字段值可以被实例化的对象修改，外部代码可以访问
 */
class Person2(var name: String, var age: Int)
//等价于
class Person22(name2: String, age2: Int){
  var name: String = name2
  var age: Int = age2
}

/*
  参数不带val或者var: 只是普通构造，不会生成类的字段，只能在类实例化使用，外部代码无法访问,两个字段是对象私有的，这类似于private[this] val字段的效果。
 */
class Person3(name:String,age:Int){
  def printDetails(): Unit = {
    println(s"Name:$name, Age:$age")
  }
}

/*
   私有主构造器：被私有的主构造器无法通过调用，可以通过伴生类对象进行调用
 */
class ClassConstructor private(var name:String){
  def printValue= println(name)
}

//伴生类对象
object ClassConstructor{
  def apply(name:String): ClassConstructor = {
    new ClassConstructor(name)
  }
}

//辅助构造器:辅助构造器是类中额外定义的构造器，用于支持多种初始化方式。每个辅助构造器必须直接或间接调用主构造器。
class Person4(val name:String, val age:Int){
  // 辅助构造器的声明不能和主构造器的声明一致,会发生错误(即构造器名重复)
  //def this(name,age) = {}

  def this(name:String) {
    this(name,0) // 调用主函数
  }

  def this(age:Int) = {
    this("jia",age)
  }
}

class Person5{
  private var name = ""
  private var age = 0

  //每一个辅助构造器都必须以主构造器或者已经定义的辅助构造器的调用开始，即需要放在辅助构造器的第一行
  def this(name: String) {
    this() //主构造
    this.name = name //self 等价于 this ; 需要出现在类的第一句
  }

  def this(name: String, age: Int) {
    this(name) //辅构造
    this.age = age
  }
}

/**
 * 对象
 */
//单例对象：Scala中没有静态方法和静态字段，可以使用object实现（object是懒加载的，且只能有一个实例），单例对象无需实例化
//可以直接通过 MySingleton.increment 访问单例对象的方法
object MySingleton {
  private var count:Int = _

  def increment(): Unit = {
    count += 1
  }
}

// 伴生对象，需要同时在一个文件中定义object和class ，并且同名。伴生对象和类可以互相访问彼此的私有成员
// apply 方法一般声明在伴生类对象中，可以用来实例化伴生类对象
class Person6 private(var name:String, var age : Int){
  def des(): Unit = {
    println(s"name: $name")
  }
}

object Person6{
  def apply(name : String, age : Int): Unit = {
    new Person6(name,age).des()
  }
}

//使用apply方法实现单例
class Person7 private(val name:String,val age:Int){
  def des(): Unit = {
    println(s"name: $name")
  }
}

object Person7 {
  private var instance: Person7 = null
  def apply(name: String,age :Int): Person7 = {
    if (instance == null){
      instance = new Person7(name,age)
    }
    instance
  }
}

/**
 * 继承：使用extend关键字，被final 修饰的父类无法被继承
 *      只有主构造器可以调用父类的构造器。辅助构造器不能直接调用父类的构造器。
 *
 * override（重写覆盖） 可以校验父类的方法或参数是否写错
 * suppe 调用在子类中父类被覆盖的方法
 */
class Father {
  private var name :String = "jia"
  val nickName :String = "jacob"
  def getName = name
  def getNickName = nickName
}

class Son extends Father{
  private var score = 100
  override def getName: String = score +" "+ super.getName //调用父类方法
}

/**
 * 类型检查和转换
 * isInstanceOf :用于检查一个对象是否属于某个特定类型 对象.isInstanceOf[对象的类型]
 * asInstanceOf :用于将一个对象强制转换为某个特定类型 对象.asInstanceOf[对象的类型]
 *
 * asInstanceOf 是强制类型转换，如果对象不是目标类型，会抛出 ClassCastException。因此，在使用 asInstanceOf 之前，通常先用 isInstanceOf 检查类型。
 */


/**
 * 父类的构造
 *   1.只有主构造器可以调用超类的构造器，辅助构造器不能直接调用超类的构造器
 *   2.如果是父类中接收的参数，在子类中接收时，不要用任何val或var来修饰了，否则会认为是子类要覆盖父类的field（字段）
 */
class Father1(val name:String)

class Son1(name:String, age:Int) extends Father1(name)

/**
 * 匿名子类：
 *   匿名子类一般都是在运行时期间定义的类。可以定义一个类的无名子类，并直接创建其对象，然后将对象的引用赋予一个变量。
 */
class Father2(val name:String){
  def getName = name
}

//val son2 = new Father2("jia"){
//  val age = 10
//  override def getName: String = name + age
//}

/**
 * 抽象类 ：如果某个类至少存在一个抽象方法或一个抽象字段（没有被初始化），则该类必须声明为abstract。
 *  1. 在Scala中，通过abstract关键字标记不能被实例化的类。
 *  2. 方法不用标记abstract，只要省掉方法体即可。
 *  3. 抽象类可以拥有抽象字段，抽象字段/属性就是没有初始值的字段
 *  4. 抽象类不能被实例化
 */
abstract class Person8{
  var name : String  //抽象字段没有初始化
  def getName        //抽象方法没有方法体
  val age  = 3
}

class Man extends Person8{
  //抽象的变量或方法必须实现（override 非必须）
  override var name: String = "jia"
  def getName: Unit = name
}

/**
 * 特质 (Traits)：类似java接口
 *   用extend来继承trait ，不用implement
 *   scala不支持对类进行多继承，但是支持多重继承trait，使用with关键字即可。
 *   没有父类
 *   class 类名 extends 特质1 with 特质2 with 特质3
 *   有父类
 *   class 类名 extends 父类 with 特质1 with 特质2 with 特质3
 */
trait ConsoleLogger{
  def log(msg:String): Unit = {
    println(msg)
  }
}

class Account{
  var balance = 0.0
}

class SavingAccount extends Account with ConsoleLogger{
  def withdraw(amount :Double): Unit = {
    if (amount>balance) log("asdf")
    else balance -= amount
  }
}

//App特质 等于main方法

/**
 * 枚举:
 *   枚举的访问:
 *     1.values返回所有枚举值
 *     2.按ID、name访问
 */

class MyEnumeration extends Enumeration{
  val Black = Value(1,"black")
  val Red,Yellow,Green = Value
}

//private val enumeration = new MyEnumeration
//println(enumeration.withName("Yellow"))  按name访问
//println(enumeration(2)) 输出 Red          按name访问

//for(c <- MyEnumeration.values){     values返回所有枚举值
//  println(c.id +":" + c)
//}



