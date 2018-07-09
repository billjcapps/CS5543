import org.apache.spark.{SparkContext, SparkConf}
object SparkWordCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils");
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)
    val result=sc.textFile("TheRaven.txt")
        .flatMap(line=>{line.split("\n")})
        .map(word=>(word,1))  //.cache()
        .reduceByKey(_+_)
        .sortBy(sort => (sort._2 * -1, sort._1) ).coalesce(1)
    result.saveAsTextFile("output")
    for ((sentence, count) <- result) println(sentence + " — " + count.toString)
    println("Total sentences: " + result.values.sum().toInt.toString)
    println("Total unique sentences: " + result.keys.count().toString)
  }
}