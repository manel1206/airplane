package org.tcb.airplanePerformance.batchProcessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;

import java.util.UUID;

public class UuidAndTimeStamp {

	// UUID.randomUUID().toString()

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();

		// "hdfs://tcbcluster/user/Manel/data/Raw/carr/0621e6bb-0595-4aea-a3cf-b10976f629ae"
		Dataset<Row> linesHdfs = spark.read().csv(args[0]);

		linesHdfs.printSchema();
		// linesHdfs.show();

		Dataset<Row> linesHdfsDF = linesHdfs.toDF(ColumnName.valueOf(args[2].toUpperCase()).getColumns());
		Dataset<Row> newLine = linesHdfsDF.withColumn("UUID", lit(UUID.randomUUID().toString()));
		// newLine.show();
		Dataset<Row> finalLine = newLine.withColumn("TimeStamp", lit(System.currentTimeMillis()));

		finalLine.write().format("com.databricks.spark.avro").save(args[1]);
	}

}


