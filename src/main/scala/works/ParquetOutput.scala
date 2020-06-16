package works

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @describe SparkSQL的文件格式化输出
 * @author 肖斌武.
 * @datetime 2020/6/16 17:30
 */
object ParquetOutput {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder()
    		.master("local[*]")
    		.config(conf)
    		.appName("ParquetOutput")
    		.getOrCreate()

		val lines = spark.read.parquet("data/etl")
		lines.createTempView("log")
		val group = spark.sql(
			"""
			  |select provincename, cityname, count(*) as cnt
			  |from log
			  |group by provincename, cityname
			  |""".stripMargin)

		// 通过partitionBy重新分区，将结果写入分区的文件夹中
		group.write.partitionBy("provincename", "cityname").json("data/city")
		spark.stop()
	}
}
