package ICE4
import org.apache.spark.{SparkConf, SparkContext}
object ICE4 {
  def main(args : Array[String]){
    // administration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val config = new SparkConf()
      .setAppName("ICE4")
      .setMaster("local[*]")
    val sc = new SparkContext(config)
    // read in data
    val textTransaction = sc.textFile("src/main/scala/ICE4/input_transaction.txt")
    val textUser = sc.textFile("src/main/scala/ICE4/input_user.txt")
    // map transactions
    val result = textTransaction.map(x => (x.split(", ")(2),x.split(", ")(1)))
      // map users to transactions
      .join(textUser.map(x => (x.split(", ")(0),x.split(", ")(3))))
      // reduce to distinct values
      .values.distinct()
    // output results
    result.saveAsTextFile("src/main/scala/ICE4/output")
      }
}