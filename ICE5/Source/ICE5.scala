package ICE5
import breeze.numerics.log2
import org.apache.spark.{SparkConf, SparkContext}
object ICE5 {
  def main(args : Array[String]){
    // administration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val config = new SparkConf()
      .setAppName("ICE5")
      .setMaster("local[*]")
    val sc = new SparkContext(config)


    val d1_wordCount = sc.textFile("src/main/scala/ICE5/docs/d1.txt").flatMap(line => line.split(" ")).map(word => (List("d1",word), 1))
      .reduceByKey(_ + _)
    val d2_wordCount = sc.textFile("src/main/scala/ICE5/docs/d2.txt").flatMap(line => line.split(" ")).map(word => (List("d2",word), 1))
      .reduceByKey(_ + _)
    val d3_wordCount = sc.textFile("src/main/scala/ICE5/docs/d3.txt").flatMap(line => line.split(" ")).map(word => (List("d3",word), 1))
      .reduceByKey(_ + _)
    val wordCounts = d1_wordCount.union(d2_wordCount).union(d3_wordCount)

    val d1_totalCount = d1_wordCount.map(x => ("d1", x._2)).reduceByKey(_ + _)
    val d2_totalCount = d2_wordCount.map(x => ("d2", x._2)).reduceByKey(_ + _)
    val d3_totalCount = d3_wordCount.map(x => ("d3", x._2)).reduceByKey(_ + _)

    val docsPerWord = wordCounts.map(x => (x._1(1),1)).reduceByKey(_ + _)
    val idf = docsPerWord.map(x => (x._1, log2(3.0/x._2)))

    val tfidf_d1 = wordCounts.map(identity).join(idf.map(x => (List("d1",x._1),x._2))).map(y => (y._1,(y._2._1 * y._2._2)))
    val tfidf_d2 = wordCounts.map(identity).join(idf.map(x => (List("d2",x._1),x._2))).map(y => (y._1,(y._2._1 * y._2._2)))
    val tfidf_d3 = wordCounts.map(identity).join(idf.map(x => (List("d3",x._1),x._2))).map(y => (y._1,(y._2._1 * y._2._2)))
    val tfidf = tfidf_d1.union(tfidf_d2).union(tfidf_d3)


    tfidf.saveAsTextFile("src/main/scala/ICE5/output")
      }
}