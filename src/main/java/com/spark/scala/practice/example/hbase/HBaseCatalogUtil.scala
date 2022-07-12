package com.spark.scala.practice.example.hbase

import com.spark.toto.TotoRecord
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseCatalogUtil {
  def getReaderCatalogStr(tableName : String): String={
    val catalogStr =
      s"""{
        |"table":{"namespace":"demo","name":"${tableName}"},
        |"rowkey":"key",
        |"columns":{
        |"col0":{"cf":"rowkey","col":"key","type":"string"},
        |"col1":{"cf":"data","col":"data","avro":"avroSchema"}
        |}
        |}""".stripMargin
    catalogStr
  }

  def getWriterCatalogStr(tableName : String): String={
    //val catalog = getReaderCatalogStr(tableName)
    null

  }

  def readHbaseTable(spark : SparkSession,tableName : String) : DataFrame={
    //import org.apache.spark.sql.execution.datasources
    val catalog = getReaderCatalogStr(tableName)
    println(catalog)
    println(TotoRecord.SCHEMA$.toString())
    val data = spark.read
      .options(Map("avroSchema"->TotoRecord.SCHEMA$.toString,
        HBaseTableCatalog.tableCatalog->getReaderCatalogStr(tableName)))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    data
  }

  def wirteToHBase(data : DataFrame,tableName : String): Unit={

  }
}
