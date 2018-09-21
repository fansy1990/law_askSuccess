package a_01_preprocess

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import util.CommonUtils._
/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 12:49
  * //@Email: fansy1990@foxmail.com
  * 过滤掉其中 特征列全部为0，目标列为-1的数据
  */
object FilterData {

  def filterData(data:DataFrame): DataFrame ={
    filterData(data,false)
  }
  def filterData(data:DataFrame,showOrNot:Boolean):DataFrame ={
    val not_include_data = data.filter(label +" = " + "-1.0")
    if(showOrNot) {
      val count = not_include_data.count()
      println("Filtered data :" + count)
      if(count < 50 ) {
        not_include_data.show(count.toInt)
      }else{
        not_include_data.show(10)
      }
    }
    data.filter(label + "!=" + -1.0)
  }
}
