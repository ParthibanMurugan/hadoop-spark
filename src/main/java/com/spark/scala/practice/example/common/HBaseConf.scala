package com.spark.scala.practice.example.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}

object HBaseConf extends Serializable {
  lazy val connection = getConnection()

  private[common] def getHbaseConf(): Configuration={
    val config = HBaseConfiguration.create()
    config.clear()
    config.set("hbase.master", "localhost:16000")
    config.addResource("/home/parthiban/hbase/hbase-2.4.13/conf/hbase-site.xml")
    (config)
  }

  private[common] def getConnection(): Connection={
    val config = getHbaseConf()

    val connection = ConnectionFactory.createConnection(config)

    (connection)
  }

  /*def listTables() : List[String]={
    val admin = new HBaseAdmin(getConnection())

    admin.listTables().map{
      (table)=>{
        String.valueOf(table.getTableName.getName)
      }
    }.toList
  }

   */
}
