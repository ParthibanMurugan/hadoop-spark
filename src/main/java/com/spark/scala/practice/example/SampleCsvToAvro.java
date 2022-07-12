package com.spark.scala.practice.example;


import com.spark.scala.practice.example.hbase.LoadDataToHBase;
import com.spark.toto.TotoRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;


public class SampleCsvToAvro {
    private static SparkSession getSparkSession(){
        return SparkSession.builder()
                .appName("HBase Load")
                .master("local[*]")
                .getOrCreate();
    }
    public static void main(String[] args){
        SparkSession spark = getSparkSession();

        Dataset<Row> csvData = spark
                .read()
                .format("csv")
                .option("header","true")
                .option("delimiter",",")
                .schema(LoadDataToHBase.getStructType())
                .load("src/main/resources/ToTo.csv");


        // SchemaConverters.toSqlType(TotoRecord.SCHEMA$).dataType();



        Dataset<Row> encodeData = csvData.map((MapFunction<Row,Row>) data->{
            return RowFactory.create(
                    data.get(data.fieldIndex("draw")),
                    data.get(data.fieldIndex("date")),
                    data.get(data.fieldIndex("colOne")),
                    data.get(data.fieldIndex("colTwo")),
                    data.get(data.fieldIndex("colThree")),
                    data.get(data.fieldIndex("colFour")),
                    data.get(data.fieldIndex("colFive")),
                    data.get(data.fieldIndex("colSix")),
                    data.get(data.fieldIndex("addNumber")),
                    data.get(data.fieldIndex("fromLast"))
            );
        }, RowEncoder.apply((StructType) SchemaConverters.toSqlType(TotoRecord.SCHEMA$).dataType()));
        StructType st = (StructType) SchemaConverters.toSqlType(TotoRecord.SCHEMA$).dataType();


        encodeData.show(false);
        encodeData.printSchema();



    }
}
