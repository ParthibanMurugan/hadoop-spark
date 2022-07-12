package com.spark.scala.practice.example.samples

import com.spark.scala.practice.example.common.SparkEnv
import com.spark.toto.TotoRecord
import org.apache.spark.sql.avro.SchemaConverters

object AvroSchemaTest extends SparkEnv {

  val schemaType = SchemaConverters.toSqlType(TotoRecord.SCHEMA$)

  println(schemaType)


}
