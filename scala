
=====================
spark-submit --class retail_db.GetRevenuePerOrder "C:\Users\vemul\IdeaProjects\sparkhello\target\scala-2.11\sparkhello_2.11-0.1.jar" local C:\itversity_datasets\retail_db\order_items C:\itversity_datasets\revenue_per_order

========================================================
How Java is platform independent?
	Bytecode, JVM makes java platform independent.

Step by step Execution of Java Program: 
	1) Whenever a program is written in java, the compiler (javac) compiles it.
	2) It generates .class file or byte code
	3) The byte code generated is non-executable code and needs an interpreter to execute.
       The interpreter is JVM and thus Bytecode is executed by JVM.
    4) Java is platform independent but JVM is platform dependent.


Scala compiles the program into byte code(java byte code). 
It is JVM based programming language.

REPL - Read Evaluate Print Loop
     - used for validate the code snippets instead running on the main application.

CLI  - TELNET, python, sqlplus, mysql


variables can be declared using keyword
val - to define immutable objects
var - to define mutable objects

Every variable is a object in scala
Every operator is a function in scala


Index starts with 0 in scala


Ex:
val a = Array(1,2,3,4)

To loop through the elements
for(v <- a) println(v)        // in imperative programming languages

a.foreach(println) 			  // in functional programming paradigm



//to check the given number is even

scala> val i = 2
scala> if(i % 2 == 0) println(i + " is even")

//to check the given number is even or odd

scala> val i = 3
i: Int = 3

scala> if (i % 2 == 0) println(i + " is even") else println(i + " is odd")
3 is odd




===================================================================================================================================================
	Day 3 Scala
====================================================================================================================================================

Functions:
==========

def helloWorld 

	If you dont have arguments you dont need to specify the brackets
and the return type is not mandatory it will automatically inherit based upon the last line of code

If function has single line of code we use =
It function has multiple lines of code we use {   }

==================
unit is same as void in java

We should not explicitly mention the datatype for variable
We should explicitly mention the datatype for passing the arguments

We dont use the return type because whatever is the datatype for the last statement that will be inherited automatically



def sumOfIntegers(lb: Int, ub: Int) = {
	var total = 0
	for(i <- (lb to ub)) total += i
	total
    }

sumOfIntegers(1,10)

======================================

Higher Order Functions

	A function that takes another function as argument is called as Higher Order functions.

Ex: Sum of squares of a given number

def sumOfSquares(lb: Int, ub: Int) = {
      var total = 0
      for(i <- (lb to ub)) total += i * i
      total
      }

sumOfSquares(1,5)


Ex: Sum of cubes of a given number

def sumOfCubes(lb: Int, ub: Int) = {
      var total = 0
      for(i <- (lb to ub)) total += i * i * i
      total
      }

sumOfCubes(1,5)


Ex: Sum of multiples of 2

def sumOfCubes(lb: Int, ub: Int) = {
      var total = 0
      for(i <- (lb to ub)) total += i * 2
      total
      }

sumOfCubes(1,5)


Usage of Higher Order Functions

Specification of the function means return type

def sum(lb: Int, ub: Int, f: Int => Int) = {
        var total = 0
        for(i <- (lb to ub)) total += f(i)
        total
      }

now creating the another function

def sqr(i: Int) = i * i


sum(1, 10, sqr)

Here sum is higher order function and sqr is lower order function

def cube(i: Int) = i * i * i

sum(1, 10, cube)





Anonymous / Lambda Functions
	
	A function without name is considered as Anonymous / Lambda Functions

Ex:
sum(1, 10, i => i * i) 
			here we are direcly mentioning the logic

sum(1, 10, (i: Int) => i * i)

Ex:
sum(1, 10, i => {
         if(i % 2 == 0) i else 0
      }
      )




==================================================================================================================================================
	Every variable is an object
 	Every operation is a function

val i = 10
val j = 20

i. + (j)		or  i + j
i.==(j)			or  i == j

	
	Here . is the prefix operator
	() in which parameters are passed is postfix operator


===================================================================================================================================================


There are 2 types of collections. They are 
									i)  Mutable collections
									ii) Immutable collections

 A mutable collection can be updated or extended in place. This means you can change, add, or remove elements of a collection as a side effect. 
 Immutable collections, by contrast, never change. You have still operations that simulate additions, removals, or updates, but those operations 
 will in each case return a new collection and leave the old collection unchanged.


All collection classes are found in the package
scala.collection. 
scala.collection.immutable
scala.collection.mutable

List is immutable
Set is mutable and immutable
Map is mutable and immutable
LinkedList is mutable
Array is mutable


Collections: group of similar element types
	Ex: List, Set, Map


	List : group of elements where duplicates may exist
		Ex: val l = List(1,2,3,4)
			Here l(0) returns the first element from the list
				 l(5) throws IndexOutOfBoundsException: 

			scala> val l = List(1,2,3,4)
			l: List[Int] = List(1, 2, 3, 4)

			scala> 5 +: l          //prepend element to the list
			res11: List[Int] = List(5, 1, 2, 3, 4)	
				here +: is the prepend and the new collection is created 

			scala> l :+ 10		//appending the element
			res13: List[Int] = List(1, 2, 3, 4, 10)	

				l.sum 		//sum the values
				l.min 
				l.max


immutable variable is different from immutable collection

scala> val l = List(1,2,3,4)
l: List[Int] = List(1, 2, 3, 4)

scala> val a = List(1, "Hello", 0.0f)
a: List[Any] = List(1, Hello, 0.0)

Any is the super class of all the classes in scala
it takes the least genreric type of all the elements

scala> val b = List(1, 0.0f)
b: List[Float] = List(1.0, 0.0)
 	Here float is the least genreric type


Data in the list can be sorted by
sortBy
sortWith
sorted

==============================================================================================================

Set: Group of unique elements. It ignores duplicates
	As set enforces the uniqueness it will not have length or Index

Set is immutable

scala> val s = Set(1,2,3,4,5,6,1,2,3,4,5,6)
s: scala.collection.immutable.Set[Int] = Set(5, 1, 6, 2, 3, 4)



To eleminate duplicates from list
use 

toSet
toList


convert from list to set

scala> val l = List(1,2,3,4,5,5,6,2,3,1)
l: List[Int] = List(1, 2, 3, 4, 5, 5, 6, 2, 3, 1)

scala> l.toSet   //it can be assigned to a variable s = l.toSet
res14: scala.collection.immutable.Set[Int] = Set(5, 1, 6, 2, 3, 4)


If you want to sort the set, you have to convert to List and perform sort APIs

scala> l
res15: List[Int] = List(1, 2, 3, 4, 5, 5, 6, 2, 3, 1)

if you want to print the elements

scala> l.foreach(println)
1
2
3
4
5
5
6
2
3
1

===========
	creating user defined function for printing elements 

scala> def myforeach(l: List[Int], f: Int => Unit) = {
     | for(i <- l) f(i)
     | }
myforeach: (l: List[Int], f: Int => Unit)Unit

scala> myforeach(l,e => println(e))
1
2
3
4
5
5
6
2
3
1





is thre any term called fullstack hadoop developer
























==========
	Docker
=====

docker run --rm -it \
		  -p 2181:2181 -p 3030:3030 -p 8081:8081 \
		  -p 8082:8082 -p 8083:8083 -p 9092:9092 \
		  -e ADV_HOST=192.168.99.100 \
		  landoop/fast-data-dev


http://192.168.99.100:3030/


ctrl c      // to exit from kafka



















val EmployeeData = Seq( Employee("Anto",   21, "Software Engineer", 2000, 56798),
Employee("Jack",   21, "Software Engineer", 2000, 93798),
Employee("Mack",   30, "Software Engineer", 2000, 28798),
Employee("Bill",   62, "CEO", 22000, 45798),
Employee("Joseph", 74, "VP", 12000, 98798),
Employee("Steven", 45, "Development Lead", 8000, 98798),
Employee("George", 21, "Sr.Software Engineer", 4000, 98798),
Employee("Matt",   21, "Sr.Software Engineer", 4000, 98798))







case class Employee (id:Int, name:String, age:Int, gender:String, level:Int, salary:Double ) {
  
  val listOfEmployees = Seq( Employee(1, "Joseph", 23, "m", 1, 50000),Employee(2, "Sharma", 25, "m", 1, 55000),Employee(3, "Varma", 26, "m", 2, 60000),
                          Employee(4,	"Aj", 27,	"m",	3, 65000),Employee(5, "Varun",	22,	"m", 1, 45000),Employee(6, "Ajay", 29, "m",	3, 95000),
                          Employee(7, "Vijay", 31, "m", 4, 125000),Employee(8, "Kaushik", 33, "m", 5, 145000),Employee(9, "Gopi", 21, "m", 1, 25000),
                          Employee(10, "Kumar", 27, "m",	3, 75000),Employee(11, "Kumari", 21, "f", 1, 35000),Employee(12, "Tina", 22, "f", 2, 45000),
                          Employee(13, "Alexa", 23, "f",	3, 55000),Employee(14, "Casey", 25, "f", 1, 25000))
                          
  val Employee_DataFrame = EmployeeData

  }

}
















































































========================================
	database connection using scala
============================================
	SQL.scala



import java.sql.DriverManager
import java.sql.Connection

//     A Scala JDBC connection example 
  

object SQL {

  def main(args: Array[String]): Unit = {
    
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/demo"
    val username = "root"
    val password = "root"
    
    // there's probably a better way to do this
    var connection:Connection = null
    
    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM employee LIMIT 5")
      while ( resultSet.next() ) {
        val id = resultSet.getString("id")
        
        println(id)
        
//        println(employee id = + id)
      }
    } catch {
      case e => e.printStackTrace
    }
        
    

  }

}


=======================================
	Employee.scala
====================================
	import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


case class Employee(id:Int, name:String, age:Int, gender:String, level:Int, salary:Double) {
  
  
  val conf = new SparkConf()
  conf.setAppName("CreatingEmpDataFrame")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext=new SQLContext(sc)
  
  
  
  val listOfEmployees = Seq( Employee(1, "Joseph", 23, "m", 1, 50000),Employee(2, "Sharma", 25, "m", 1, 55000),Employee(3, "Varma", 26, "m", 2, 60000),
                          Employee(4,	"Aj", 27,	"m",	3, 65000),Employee(5, "Varun",	22,	"m", 1, 45000),Employee(6, "Ajay", 29, "m",	3, 95000),
                          Employee(7, "Vijay", 31, "m", 4, 125000),Employee(8, "Kaushik", 33, "m", 5, 145000),Employee(9, "Gopi", 21, "m", 1, 25000),
                          Employee(10, "Kumar", 27, "m",	3, 75000),Employee(11, "Kumari", 21, "f", 1, 35000),Employee(12, "Tina", 22, "f", 2, 45000),
                          Employee(13, "Alexa", 23, "f", 3, 55000),Employee(14, "Casey", 25, "f", 1, 25000))
                       
  val empData=sqlContext.createDataFrame(listOfEmployees)
  empData.printSchema
}



=======================================
	Emp.scala
=========================================


case class Emp(id: Int, name: String, address:String) {
  
  
  val customerA = Emp(1, "Robin", "Taipei")
  val customerB = Emp(2, "Jiaming", "Kaohsiung")
  val customerC = Emp(3, "Lin", "Pentung")
  



    println(List(customerA, customerB, customerC))
    println(customerA.name) 
  

  
}
  
    
                          
                          
                   





 ====================================
 	Demo.scala
 ====================================


object Demo extends App {
  println("Hello")
  System.out.println("Hello Scala")
}



==================================
	sbt version


=========================================================
sbt

sbt package  //to generate jar files

sbt run  // if the project has only 1 main function, to run the jar file

sbt run argumentname  //to run jar and pass arguments

sbt "run-main HelloWorld Nikhil"  // if the main function has mutiple objects use this way
