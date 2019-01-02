import org.apache.spark.sql.SparkSession
import scala.math.random

/**
  * Sample Spark application.
  *  Calculates volume of a sphere (r = 1) using the Monte Carlo method.
  */
object SparkApplication {

  case class Point(x: Double, y: Double, z: Double)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .appName("Spark Pi")
      .config("spark.driver.host", "172.18.0.4" ) //"172.18.0.2", "172.21.0.2"
      //.config("spark.local.ip","172.18.0.2")
      .config("spark.driver.port", "20002")
      .config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.blockManager.port", "6060")
      .getOrCreate()

    val n = 2950000
    val sc = sparkSession.sparkContext
    sc.setLogLevel("DEBUG")

    val pointsRdd = sc.parallelize(1 to n)
      .map(_ => Point(random * 2 - 1, random * 2 - 1, random * 2 - 1))

    val pointsInsideSphereRdd =
      pointsRdd.filter(point => point.x * point.x + point.y * point.y + point.z * point.z <= 1)

    val insideToAllRatio = pointsInsideSphereRdd.count().toDouble / pointsRdd.count().toDouble
    val cubeSize = 2 * 2 * 2

    println(s"Result: ${insideToAllRatio * cubeSize}")
    sparkSession.stop()
  }
}
