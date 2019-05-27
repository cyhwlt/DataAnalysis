package com.datacaculate

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object HiveSpark {

  def main(args: Array[String]): Unit = {
//  val sql = "select bc.channelid,bc.channelname,bc.createuser,ci.imgid,ci.imgurl from zhilian_test.basechannel bc join zhilian_test.channelimg ci on bc.channelid=ci.channelid where bc.channelid='001005d0-89a5-45ed-b5ee-42feef26ac97'"
  val sql = "select * from zhilian_test.equipment where platfromid='ff658af2-77e4-4983-a515-f8511d7e550c'"
  val frame = sqlCaculate(sql)
}

  def sqlCaculate(sql: String):  util.ArrayList[Map[String,Object]] = {
    val conf = new SparkConf()
    conf.setAppName("Hivespark").setMaster("local")
    val sc = new SparkContext(conf)
    val list = new util.ArrayList[Map[String,Object]]
    try{
      // 通过SparkSession
      val hive = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
      val frame = hive.sql(sql)
      frame.show()
      frame.collect().foreach(row => {
        val map = row.getValuesMap(frame.columns)
        list.add(map)
      })
      println(">>>>>>>>>>>" + list)
    } catch{
      case e:Exception => println(e)
    } finally {
      sc.stop()
    }
    return list
  }
}
