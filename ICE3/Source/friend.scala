import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object mutualFriends {
  def main(args : Array[String]){
    // administration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val config = new SparkConf()
      .setAppName("Mutual Friends")
      .setMaster("local[*]")
    val sc = new SparkContext(config)

    // read in data and convert to KVP (Person->[List of Friends])
    val textFile = sc.textFile("src/main/scala/friends.txt")
    val mutualFriends = textFile.map(x => (x.split(" -> ")(0), x.split(" -> ")(1).split(" ").toList))

    // MapReduce
    val mf = mutualFriends.map(x => (x._1.flatMap(y => x._2.map(z => List(y.toString,z).sorted)).toList,x._2))
      .flatMap(x => x._1.map(y => (y,x._2)))
        .reduceByKey(_.intersect(_))

    // Output results
    mf.saveAsTextFile("src/main/scala/output.txt")













  }



}