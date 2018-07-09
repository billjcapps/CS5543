package ICE7
import org.apache.spark.{SparkConf, SparkContext}
object ICE7 {
  def main(args: Array[String]) {
    // administration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val config = new SparkConf()
      .setAppName("ICE7")
      .setMaster("local[*]")
    val sc = new SparkContext(config)
    var pr = sc.textFile("src/main/scala/ICE7/PageRanks.txt").map(x => (x.split('|')(0).split(' ')(0),
      (x.split('|')(0).split(' ')(1).toDouble, x.split('|')(1))))
    val n = pr.count()
    val d = 0.85

    /*
    var check1 = pr.values.keys.sum().toDouble
    var check2 = -1.0
    while (((check1-check2)*(check1-check2)) > 0.1) {
      pr = pr.flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
        .union(pr.map(x => (x._1, 0)))
        .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n/n))
        .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))
      check2 = check1
      check1 = pr.values.keys.sum()
      }
*/
    pr = pr.flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

      .flatMap(x => (x._2._2.split(" ").map(y => (y, ((x._2._1) / x._2._2.split(" ").length)))))
      .union(pr.map(x => (x._1, 0)))
      .reduceByKey(_ + _).map(x => (x._1, x._2 * d + (1 - d)/n))
      .join(pr).map(x => (x._1, (x._2._1, x._2._2._2)))

    pr.saveAsTextFile("src/main/scala/ICE7/output")
  }
}