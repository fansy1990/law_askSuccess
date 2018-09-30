package a_01_preprocess

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**suling
  * 读取Hive数据库中全量数据
  */
object ReadHiveDB {

  def getData(): DataFrame = {
 val sparkSession = SparkSession.builder().appName("HiveCaseJob").master("local[*]").enableHiveSupport().getOrCreate()
//   // sparkSession.sql("select userid,timestamp_format,fullurl,fullurlid,pagetitle,pagetitlecategoryid,pagetitlecategoryname,pagetitlekw from law_init1.lawtime_gt_one_distinct")
//   val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("dff").set("fs.defaultFs","hdfs://192.168.111.74:8020"))
//
    val hiveContext = new HiveContext(sparkSession.sparkContext)
//    hiveContext.sql("select userid,timestamp_format,fullurl,fullurlid,pagetitle,pagetitlecategoryid,pagetitlecategoryname,pagetitlekw from law_init1.lawtime_gt_one_distinct")
  hiveContext.sql("select userid,timestamp_format,fullurl,fullurlid,pagetitle,pagetitlecategoryid,pagetitlecategoryname,pagetitlekw from law_init1.lawtime_gt_one_distinct")
  }
//
//  def main(args: Array[String]): Unit = {
//    val data = getData()
//    data.show(2)
//  }
//val url = "jdbc:hive2://192.168.111.240:10000/law_init1"
//  val user = "root"
//  val password = "root"
//  val table = "law_init1.lawtime_gt_one_distinct"
//  val driver ="org.apache.hive.jdbc.HiveDriver"
//
//  def getData(): DataFrame = {
//    val  properties = new Properties()
//    properties.put("user",user)
//    properties.put("password",password)
//    properties.put("driver",driver)
//    CommonUtils.getSparkSession().read.jdbc(url,table,properties)
//  }

  def main(args: Array[String]): Unit = {
    val data = getData()
    println("数据量："+data.count())
    data.show(20)
//    Class.forName(driver)
//
//    val con = DriverManager.getConnection(url, user, password)
//    val stmt = con.createStatement();
//    val sql = "select * from "+table;
//    System.out.println("Running: " + sql);
//    val res = stmt.executeQuery(sql);
//    println(res.first())

  }
}
