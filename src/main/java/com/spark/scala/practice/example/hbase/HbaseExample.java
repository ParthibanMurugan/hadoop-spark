package com.spark.scala.practice.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.Iterator;

public class HbaseExample {

    public static void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();


        TableName table1 = TableName.valueOf("demo:employee");
        String family1 = "name";
        String family2 = "emp_id";


        HTableDescriptor desc = new HTableDescriptor(table1);
        desc.addFamily(new HColumnDescriptor(family1));

        desc.addFamily(new HColumnDescriptor(family2));
        admin.createTable(desc);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Hello ");

        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.master", "localhost:16000");
        config.addResource("/home/parthiban/hbase/hbase-2.4.13/conf/hbase-site.xml");
        //HBaseAdmin.available(config);



        Connection connection = ConnectionFactory.createConnection(config);

        //HBaseContext context =
        TableName tableName = TableName.valueOf("demo:employee");

        Table table = connection.getTable(tableName);
        Scan scan = new Scan();

        scan.addColumn(Bytes.toBytes("name"),Bytes.toBytes("first"));
        scan.addColumn(Bytes.toBytes("name"),Bytes.toBytes("last"));

        ResultScanner rs = table.getScanner(scan);

        Iterator<Result> resultIterator = rs.iterator();

        while(resultIterator.hasNext()){
            Result result = resultIterator.next();

            System.out.println(result);

            result.listCells().forEach(cell->System.out.println(new String(cell.getRowArray())));
        }



    }
}
