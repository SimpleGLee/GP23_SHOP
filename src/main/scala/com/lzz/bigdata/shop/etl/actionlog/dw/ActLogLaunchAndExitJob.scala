package com.lzz.bigdata.shop.etl.actionlog.dw

import com.lzz.bigdata.shop.constant.ShopConstant
import com.lzz.bigdata.shop.etl.actionlog.ActionLogHelper.ActionLogColumnsODSHelper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Write By SimpleLee 
  * On 2019-十月-星期六
  * 17-35
  */
class ActLogLaunchAndExitJob {}
object ActLogLaunchAndExitJob {
  // TODO:日志
  private val logger: Logger = LoggerFactory.getLogger(ActLogLaunchAndExitJob.getClass)

  /**
    * 行为日志：启动|退出
    * @param spark
    * @param appName
    * @param bdp_day
    */
  def handleLaunchAndExit(spark:SparkSession,appName:String,bdp_day:String)={
    // TODO:获取当前时间
    val begin = System.currentTimeMillis()

    try {
      // TODO:引入隐式转换
      import spark.implicit._
      import org.apache.spark.sql.functions._

      // TODO:设置缓存级别
      val storagelevel: StorageLevel = ShopConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      // TODO:日志数据（行为日志）--ODS层
      val odsColumns: ArrayBuffer[String] = ActionLogColumnsODSHelper.selectActionLogColumns()

      // TODO:当天数据
      val odsCondition = col(ShopConstant.DEF_PARTITION) === lit(bdp_day)

      // TODO:获取数据
      SparkHelper.



    }catch {
      case e:Exception => {
        println(s"ActLogLaunchAndExitJob.handleActionLogJob occur exception：app=[$appName],date=[${bdp_day}], msg=$e")
        logger.error(e.getMessage, e)
      }
    }finally {
      println(s"ActLogLaunchAndExitJob.handleActionLogJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }


  }


}
