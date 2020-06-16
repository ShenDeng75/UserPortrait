import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @describe
 * @author 肖斌武.
 * @datetime 2020/6/16 11:07
 */
object Study {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.master("local[*]")
			.appName("etl").getOrCreate()

		val list1 = List(List(1,2,3,4,5,6,7,8,9),List(1,2,3,4,5,6,7,8,9))
		val rdd = spark.sparkContext.parallelize(list1)
		val value: RDD[(Int, List[Int])] = rdd.map((1, _))
		value.reduceByKey((list1,list2)=>list1.zip(list2).map(x=>x._1+x._2)).foreach(println)
	}
}
