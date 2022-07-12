package com.spark.scala.practice.example.hbase

import com.spark.scala.practice.example.common.{HBaseConf, SparkEnv}
import com.spark.toto.TotoRecord
import com.typesafe.config.Config
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Row}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.apache.spark.sql.execution.datasources.hbase.types.AvroSerde
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

import scala.collection.mutable

object LoadDataToHBase extends SparkEnv {

  def readToToData(): DataFrame ={
    val data = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .schema(getStructType)
      .load("src/main/resources/ToTo.csv")

    data.show(false)

    data.printSchema()

    val columns = Seq("draw", "date", "colOne", "colTwo", "colThree", "colFour", "colFive", "colSix", "addNumber", "fromLast")

    data.selectExpr(columns: _*)
  }

  def getStructType: StructType = {

    val structType = StructType(Array(
      StructField("draw", StringType, false),
      StructField("date", DateType, false),
      StructField("colOne", IntegerType, false),
      StructField("colTwo", IntegerType, false),
      StructField("colThree", IntegerType, false),
      StructField("colFour", IntegerType, false),
      StructField("colFive", IntegerType, false),
      StructField("colSix", IntegerType, false),
      StructField("addNumber", IntegerType, false),
      StructField("fromLast", StringType, true),
      StructField("same_as_day", StringType, true),
      StructField("odd", IntegerType, false),
      StructField("even", IntegerType, false),
      StructField("1_to_10", IntegerType, false),
      StructField("11_to_20", IntegerType, false),
      StructField("21_to_30", IntegerType, false),
      StructField("31_to_40", IntegerType, false),
      StructField("41_to_50", IntegerType, false),
      StructField("div_1_prize", DoubleType, false),
      StructField("div_1_winner", IntegerType, false),
      StructField("div_2_prize", DoubleType, false),
      StructField("div_2_winner", IntegerType, false),
      StructField("div_3_prize", DoubleType, false),
      StructField("div_3_winner", IntegerType, false),
      StructField("div_4_prize", DoubleType, false),
      StructField("div_4_winner", IntegerType, false),
      StructField("div_5_prize", DoubleType, false),
      StructField("div_5_winner", IntegerType, false),
      StructField("div_6_prize", DoubleType, false),
      StructField("div_6_winner", IntegerType, false),
      StructField("div_7_prize", DoubleType, false),
      StructField("div_7_winner", IntegerType, false)
    ))
    structType
  }
  println("Welcome to Scala")
  //println(getAllConf)

  val readFormat = conf.getString("spark.read.conf.format")

  val optionsConf = conf.getConfig("spark.read.conf.options")


  val data_1 = readToToData

  val connection = HBaseConf.connection

  data_1.foreachPartition(data => {
    // val admin = connection.getAdmin
    val table = connection.getTable(TableName.valueOf("demo:toto"))
    data.map(row => {
      val record = TotoRecord.newBuilder()
        .setDraw(row.getString(row.fieldIndex("draw")))
        .setDate(row.getDate(row.fieldIndex("date")).toLocalDate.toEpochDay)
        .setColOne(row.getInt(row.fieldIndex("colOne")))
        .setColTwo(row.getInt(row.fieldIndex("colTwo")))
        .setColThree(row.getInt(row.fieldIndex("colThree")))
        .setColFour(row.getInt(row.fieldIndex("colFour")))
        .setColFive(row.getInt(row.fieldIndex("colFive")))
        .setColSix(row.getInt(row.fieldIndex("colSix")))
        .setAddNumber(row.getInt(row.fieldIndex("addNumber")))
        .setFromLast(row.getString(row.fieldIndex("fromLast")))
        .build()
      record
    }).foreach(record => {
      val put = new Put(Bytes.toBytes(record.getDraw))

//      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("data"), AvroSerde.serialize(record,TotoRecord.SCHEMA$))

      table.put(put)
    })
  })

}
