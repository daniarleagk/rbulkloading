package scala.test

import java.io.IOException
import java.io.DataInputStream
import java.io.FileInputStream
import core.scala.RectangleTuple

object ReadRectangles {
  
  def readDoublePointRecs(dimension : Int, dis : DataInputStream) : Stream[RectangleTuple] = {
      val leftP =  new  Array[Double](dimension)
      val rightP = new Array[Double](dimension)
      for (i <- 0 until dimension){
        leftP(i) = dis.readDouble()
      }
      for (i <- 0 until dimension){
        rightP(i) = dis.readDouble()
      }
      new RectangleTuple(leftP, rightP) #:: readDoublePointRecs(dimension, dis)
  }

  
  def main(args: Array[String]){
    
    val dis = new DataInputStream(new FileInputStream("/home/da/data/rea02.rec"))
    try{
      readDoublePointRecs(2, dis).take(10).foreach { x => println(x) }
    }catch {
      case e : IOException => e.printStackTrace();
    }finally {
      dis.close()
    }
  }
  
}