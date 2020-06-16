package works.terminal

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import works.LocationRpt

/**
 * @describe 运营的请求指标
 * @author 肖斌武.
 * @datetime 2020/6/16 20:39
 */
object OperationRpt {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder()
			.master("local[*]")
			.config(conf)
			.appName("Operation")
			.getOrCreate()
		val dfLog: DataFrame = spark.read.parquet("data/etl")
		dfLog.createTempView("log")

		// as 后的字段名称要与数据库表的字段名称对应
		val location = spark.sql(
			"""
			  |select
			  |ispname,
			  |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as rawRequest,
			  |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as validRequest,
			  |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adRequest,
			  |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as joinCompete,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as joinWin,
			  |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as show,
			  |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as client,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dspWin,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dspadp
			  |from log
			  |group by ispname
			  |""".stripMargin)

		//		location.write.partitionBy("ispname").json("data/operation")

		val properties = new Properties()
		properties.load(LocationRpt.getClass.getClassLoader.getResourceAsStream("settings.properties"))
		location.repartition(1).write.mode(SaveMode.Append)
			.jdbc(properties.getProperty("url"), properties.getProperty("operationTable"), properties)

		spark.close()
	}
}
