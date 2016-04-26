package quickdraw

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import java.io._
import javax.imageio.ImageIO
import java.awt.image.BufferedImage

object reduce {
  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    //Function to load and format image data
    def getImgData(name: String): Array[String] = {

      val num = 4

      val img = ImageIO.read(new File(name + ".png"))

      // obtain width and height of image
      val w = img.getWidth
      val h = img.getHeight

      var lines = new Array[String](w / num + 1)

      // copy pixels (mirror horizontally)
      for (x <- 0 until w by num) {
        var line = "";
        for (y <- 0 until h by num) {
          if (y + num < h && x + num < w) {
            val block1 = img.getRGB(x, y).toString() + img.getRGB(x + 1, y).toString() + img.getRGB(x, y + 1).toString() + img.getRGB(x + 1, y + 1).toString()
            val block2 = img.getRGB(x + 2, y).toString() + img.getRGB(x + 3, y).toString() + img.getRGB(x + 2, y + 1).toString() + img.getRGB(x + 3, y + 1).toString()
            val block3 = img.getRGB(x, y + 2).toString() + img.getRGB(x + 1, y + 2).toString() + img.getRGB(x, y + 3).toString() + img.getRGB(x + 1, y + 3).toString()
            val block4 = img.getRGB(x + 2, y + 2).toString() + img.getRGB(x + 3, y + 2).toString() + img.getRGB(x + 2, y + 3).toString() + img.getRGB(x + 3, y + 3).toString()

            line = line + block1 + "_" + block2 + "_" + block3 + "_" + block4 + ",";

          }
        }

        //        println("x/4: " + x / 4)
        //        println("lines length: " + lines.length)
        //        println(line)
        //        println(x)
        //        println(w)

        lines(x / num) = line

      }

      lines

    }

    // Load pattern data
    val patternImgData = sc.parallelize(getImgData("testImg2_pattern1_1"))

    //val pattern1 = patternImgData.filter(_.length > 0).flatMap(_.split(",")).map { word => (word, 1) }.reduceByKey(_ + _).collect()
    val pattern1 = patternImgData.filter(_.length > 0).flatMap(_.split(",")).flatMap(_.split("_")).map { word => (word, 1) }.reduceByKey(_ + _).collect()

    val broadcastVar = sc.broadcast(pattern1)

    //    for (x <- 0 until 6) {
    //      patternImgData.filter(_.length > 0).map(_.split(",")(x)).map { word => (word, 1) }.reduceByKey(_ + _).filter(isEqual)
    //    }

    def findPatterns(line: (String, Int)): Boolean = {

      //val patternImgDataA = sc.parallelize(lines)

      // println("Finding Patterns")

      var found = false

      def isEqual(lineA: (String, Int)): Boolean = {
        if (line._1 == lineA._1) {
          println("FOUND !!!!!!!!!!!!!!!!!!")
          println("line._1:   " + line._1)
          println("lineA._1:   " + lineA._1)
          found = true
        }

        false
      }

      broadcastVar.value.foreach(isEqual)

      if (found) {
        true
      } else {
        false
      }
      //      sc.parallelize(line.split(","), line.split(",").length).map { word => (word, 1) }.reduceByKey(_ + _).collect().foreach(println)

      // line.map(_.split(",")(x)).map { word => (word, 1) }.reduceByKey(_ + _).collect().foreach(println)

    }

    val testImgData = sc.parallelize(getImgData("testImg2"))

    //Reduce image data and find like patterns
    //val filteredTestData = testImgData.filter(_.length > 0).flatMap(_.split(",")).map { word => (word, 1) }.reduceByKey(_ + _).filter(findPatterns).collect()
    val filteredTestData = testImgData.filter(_.length > 0).flatMap(_.split(",")).flatMap(_.split("_")).map { word => (word, 1) }.reduceByKey(_ + _).filter(findPatterns).collect()

    //print out like patterns
    filteredTestData.foreach(println)

  }
}