import org.apache.spark.{SparkContext, SparkConf}
import java.io.{File, PrintWriter}
object Lab2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)

    val supervisors = sc.textFile("supervisors.txt")
      .map(x => (x.split(" ")(0),x.split(" ")(1) ))

    def isOdd(n:Int): Boolean = n % 2 == 1

    val badgeScans = sc.textFile("badgeScans.txt")
      .map(x => (x,1)).reduceByKey(_ + _).filter(x => isOdd(x._2))
      .join(supervisors).map(x => (x._2._2,x._1))
      .groupByKey()
      .collect()

    val output = new PrintWriter(new File("output.txt"))
    for ((sup,emps) <- badgeScans) {
      output.write("*****  " + sup + "  *****\n")
      for (emp <- emps) output.write(emp + "\n")
      output.write("\n")
    }
    output.close()

    //for ((sup, emp) <- supervisors)  println(emp + " — " + sup)



  }
}
