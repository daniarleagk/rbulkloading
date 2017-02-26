package scala.test

import org.apache.batik.svggen.SVGGraphics2D
import core.scala.RectangleTuple
import java.awt.Color

import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.SVGGraphics2D;
import org.apache.batik.dom.GenericDOMImplementation;
import org.w3c.dom.DOMImplementation
import org.w3c.dom.Document
import java.io.IOException
import java.io.FileWriter
import java.io.PrintWriter
import java.io.DataInputStream
import java.io.FileInputStream

case class SVGConf(padding: Int, max : Int,  x: Double, y: Double, scaleX: Double, scaleY: Double, generator: SVGGraphics2D)

object SVGGenerator {

  def drawRect(conf: SVGConf)(rec: RectangleTuple): Unit = {
    val pointLeft = rec.left;
    val pointRight = rec.right;
    var minx = pointLeft(0);
    var miny = pointLeft(1);
    var maxx = pointRight(0);
    var maxy = pointRight(1);
    val d1 = maxx - minx;
    val d2 = maxy - miny;
    minx = (minx + conf.x) * conf.scaleX 
    miny = (miny + conf.y) * conf.scaleY
    maxx = (d1 * conf.scaleX) + minx;
    maxy = (d2 * conf.scaleY) + miny;
    if (maxx == minx && maxy == miny) {
      maxx = 1 + minx;
      maxy = 1 + miny;
    }
    conf.generator.setColor(Color.BLACK);
    conf.generator.drawRect(minx.toInt + conf.padding,
      -maxy.toInt + (conf.padding  + conf.max), maxx.toInt - minx.toInt, maxy.toInt - miny.toInt);
  }

  def main(args: Array[String]): Unit = {
    val dom: DOMImplementation = GenericDOMImplementation.getDOMImplementation();
    val doc: Document = dom.createDocument(null, "svg", null);
    val generator = new SVGGraphics2D(doc);
    //
    val scale = 1600
    val deltaX = 1.0
    val deltyY = 1.0
    val x = 0.0
    val y = 0.0
    //
    val conf = new SVGConf(20, scale , x, y, scale, scale, generator)
    //
    val dis = new DataInputStream(new FileInputStream("/home/da/data/rea02.rec"))
    try{
      ReadRectangles.readDoublePointRecs(2, dis).take(100000).foreach (drawRect(conf))
    }catch {
      case e : IOException => e.printStackTrace();
    }finally {
      dis.close()
    }
    //
    val file = new PrintWriter(new FileWriter("/home/da/data/rea02.svg"));
    try {
      generator.stream(file)
    } catch {
      case ioe: IOException => ioe.printStackTrace();

    } finally {
      file.close()
    }
  }
}