import core.scala.RectangleTuple


object Test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  val rec = new RectangleTuple(Array[Double](2.0,4), Array[Double](2.0, 2))
                                                  //> rec  : core.scala.RectangleTuple = LeftPoint: 2.0, 4.0 
                                                  //|  RightPoint: 2.0, 2.0
  println(rec)                                    //> LeftPoint: 2.0, 4.0 
                                                  //|  RightPoint: 2.0, 2.0
  val v = Array(1,2,3)                            //> v  : Array[Int] = Array(1, 2, 3)
  val c = v.clone()                               //> c  : Array[Int] = Array(1, 2, 3)
  v(0) = 10
  println(v.mkString(", "))                       //> 10, 2, 3
  println(c.mkString(", "))                       //> 1, 2, 3
  
  
  
}