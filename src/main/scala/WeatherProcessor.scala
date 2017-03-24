//To Process Data from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/

import com.cloudera.sa.ISHProcessor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by daniel.tomes on 3/24/17.
  */
object WeatherProcessor extends App {

  var sInFileName = ""
  var sOutFileName = ""
  var inputDir = ""
  var outputDir = ""
  var inputPath : Path = _
  var outputPath : Path = _
  var conf : Configuration = _

  //  setUpEnv
  //  val fs = FileSystem.get(conf)


  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    //    .master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
    .config("spark.hadoop.mapred.output.compress","true")
    .appName("Weather Processor")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val sqlContext = SQLContext
  import spark.implicits._

  val fs = FileSystem.get(sc.hadoopConfiguration)

  // Process args
  if (args.length <= 1) System.exit(77)
  if (args.length == 2) {
    inputDir = args(0)
    outputDir = args(1)

  }

  println("The Input Directory is set as " + inputDir)
  //  val processor = new ISHProcessor

  //  "/user/tomesd/data/weather/sample/input"
  //  "/user/tomesd/data/weather/raw/2015"
  val recordsRDD = sc.textFile(inputDir).map(line => {val proc = new ISHProcessor; proc.processISHLine(line)}).map(x => x.split(" +")).map(record => {
    val goodRecord : ListBuffer[String] = ListBuffer.empty[String]
    val badRecord : ListBuffer[Array[String]] = ListBuffer.empty[Array[String]]
    for (dataPoint <- record.indices) {
      goodRecord.append(record(dataPoint).replaceAll("\\*", ""))
    }
    Row.fromSeq(goodRecord)
  }).filter(row => row.length == 33)


  val toTSUDF = udf {ts: String => {
    val sb = new StringBuilder()
    sb.append(ts.substring(0,4))
    sb.append("-")
    sb.append(ts.substring(4,6))
    sb.append("-")
    sb.append(ts.substring(6,8))
    sb.append(" ")
    sb.append(ts.substring(8,10))
    sb.append(":")
    sb.append(ts.substring(10,12))
    sb.append(":00")
    sb.toString()
  }}

  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

  val tempDF_Raw = spark.createDataFrame(recordsRDD, getISHRawSchema)
  tempDF_Raw.withColumn("obs_ts", toTSUDF($"obs_ts_raw")).withColumn("p_yyyymm", $"obs_ts_raw".substr(0,6)).write.mode(SaveMode.Overwrite).insertInto("weather.isd_raw")

  //  def setUpEnv = {
  //    conf = new Configuration()
  //    conf.addResource(new File("src/main/resources/hdfs-site.xml").getAbsoluteFile.toURI.toURL)
  //    conf.addResource(new File("src/main/resources/core-site.xml").getAbsoluteFile.toURI.toURL)
  //    conf.addResource(new File("src/main/resources/yarn-site.xml").getAbsoluteFile.toURI.toURL)
  //  }

  def getISHRawSchema : StructType = {

    StructType(Seq(
      StructField("USAF", StringType, true),
      StructField("WBAN", StringType, true),
      StructField("obs_ts_raw", StringType, true),
      StructField("dir", StringType, true),
      StructField("spd", StringType, true),
      StructField("gus", StringType, true),
      StructField("clg", StringType, true),
      StructField("skc", StringType, true),
      StructField("L", StringType, true),
      StructField("M", StringType, true),
      StructField("H", StringType, true),
      StructField("vsb", StringType, true),
      StructField("mw1", StringType, true),
      StructField("mw2", StringType, true),
      StructField("mw3", StringType, true),
      StructField("mw4", StringType, true),
      StructField("aw1", StringType, true),
      StructField("aw2", StringType, true),
      StructField("aw3", StringType, true),
      StructField("aw4", StringType, true),
      StructField("W", StringType, true),
      StructField("temp", StringType, true),
      StructField("dewp", StringType, true),
      StructField("slp", StringType, true),
      StructField("alt", StringType, true),
      StructField("stp", StringType, true),
      StructField("max", StringType, true),
      StructField("min", StringType, true),
      StructField("pcp01", StringType, true),
      StructField("pcp06", StringType, true),
      StructField("pcp24", StringType, true),
      StructField("pcpxx", StringType, true),
      StructField("sd", StringType, true)
    ))
  }
  //  def convertTS(ts: String) : String = {
  //    val sb = new StringBuilder()
  //    sb.append(ts.substring(0,4))
  //    sb.append("-")
  //    sb.append(ts.substring(5,7))
  //    sb.toString()
  //  }

}
