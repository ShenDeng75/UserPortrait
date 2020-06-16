package etl

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @describe 将text格式的log数据转为parquet格式，KryoSerializer序列化，使用snappy压缩的文件
 * @author 肖斌武.
 * @datetime 2020/6/16 15:08
 */
object Log2Parquet {
	def main(args: Array[String]): Unit = {
		// 设置序列化方式
		val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder()
			.master("local[*]")
			.config(conf) // 加载配置信息
			.appName("etl").getOrCreate()

		val lines = spark.sparkContext.textFile("data/textLog.log")

		// 导入String转Int和Double的隐式转换
		import utils.Implicits._

		val words: RDD[Log] = lines.filter(x => x.split(",", x.length).length >= 85).map(x => {
			val arg = x.split(",", x.length)
			Log(arg(0), arg(1), arg(2), arg(3), arg(4), arg(5), arg(6), arg(7), arg(8), arg(9),
				arg(10), arg(11), arg(12), arg(13), arg(14), arg(15), arg(16), arg(17), arg(18), arg(19),
				arg(20), arg(21), arg(22), arg(23), arg(24), arg(25), arg(26), arg(27), arg(28), arg(29),
				arg(30), arg(31), arg(32), arg(33), arg(34), arg(35), arg(36), arg(37), arg(38), arg(39),
				arg(40), arg(41), arg(42), arg(43), arg(44), arg(45), arg(46), arg(47), arg(48), arg(49),
				arg(50), arg(51), arg(52), arg(53), arg(54), arg(55), arg(56), arg(57), arg(58), arg(59),
				arg(60), arg(61), arg(62), arg(63), arg(64), arg(65), arg(66), arg(67), arg(68), arg(69),
				arg(70), arg(71), arg(72), arg(73), arg(74), arg(75), arg(76), arg(77), arg(78), arg(79),
				arg(80), arg(81), arg(82), arg(83), arg(84)
			)
		})
		import spark.implicits._
		val df = words.toDF()
		df.repartition(1).write.parquet("data/etl")
//		df.show()
	}
}

case class Log(sessionid: String,
               advertisersid: Int,
               adorderid: Int,
               adcreativeid: Int,
               adplatformproviderid: Int,
               sdkversion: String,
               adplatformkey: String,
               putinmodeltype: Int,
               requestmode: Int,
               adprice: Double,
               adppprice: Double,
               requestdate: String,
               ip: String,
               appid: String,
               appname: String,
               uuid: String,
               device: String,
               client: Int,
               osversion: String,
               density: String,
               pw: Int,
               ph: Int,
               longs: String,
               lat: String,
               provincename: String,
               cityname: String,
               ispid: Int,
               ispname: String,
               networkmannerid: Int,
               networkmannername: String,
               iseffective: Int,
               isbilling: Int,
               adspacetype: Int,
               adspacetypename: String,
               devicetype: Int,
               processnode: Int,
               apptype: Int,
               district: String,
               paymode: Int,
               isbid: Int,
               bidprice: Double,
               winprice: Double,
               iswin: Int,
               cur: String,
               rate: Double,
               cnywinprice: Double,
               imei: String,
               mac: String,
               idfa: String,
               openudid: String,
               androidid: String,
               rtbprovince: String,
               rtbcity: String,
               rtbdistrict: String,
               rtbstreet: String,
               storeurl: String,
               realip: String,
               isqualityapp: Int,
               bidfloor: Double,
               aw: Int,
               ah: Int,
               imeimd5: String,
               macmd5: String,
               idfamd5: String,
               openudidmd5: String,
               androididmd5: String,
               imeisha1: String,
               macsha1: String,
               idfasha1: String,
               openudidsha1: String,
               androididsha1: String,
               uuidunknow: String,
               userid: String,
               iptype: Int,
               initbidprice: Double,
               adpayment: Double,
               agentrate: Double,
               lomarkrate: Double,
               adxrate: Double,
               title: String,
               keywords: String,
               tagid: String,
               callbackdate: String,
               channelid: String,
               mediatype: Int
              )  {}