package com.spark.scala.practice.example.common

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkEnv extends App {
  lazy val conf = ConfigFactory.load("application.conf")
  lazy val spark = getSession

  def getVersionInfo: Seq[String] = {
    val sc = spark.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")

    val versionInfo =
      s"""
         |scala version :$scalaVersion
         |spark version : ${sc.version}
         |spark master :${sc.master}
        |default parallelism: ${sc.defaultParallelism}
        |""".stripMargin

    versionInfo.split("/n")

  }
  def getSession : SparkSession={
    val sparkSession = SparkSession.builder()
      .appName("Spark-Practice")
      .master("local[*]")
      .getOrCreate();

    sparkSession
  }

  def getAllConf : String={
    getSession
      .conf
      .getAll
      .map{
        case(k,v) => "Key:[%s] Value: [% s]".format(k,v)
      }.mkString("","\n","\n")
  }
}
