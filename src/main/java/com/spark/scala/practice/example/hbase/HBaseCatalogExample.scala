package com.spark.scala.practice.example.hbase

import com.spark.scala.practice.example.common.{HBaseConf, SparkEnv}
import org.apache.hadoop.hbase.spark.HBaseContext

object HBaseCatalogExample extends SparkEnv{

  val connection = HBaseConf.connection

  //val data = LoadDataToHBase.readToToData()

  val data = HBaseCatalogUtil.readHbaseTable(spark,"toto")

  data.show(false)

  //val context = new HBaseContext(spark.sparkContext,connection.getConfiguration)



}
