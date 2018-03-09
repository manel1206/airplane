package org.tcb.airplanePerformance.batchProcessing;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataClean {

	public static void main(String[] args) {

		// otp Files
		SparkSession jss = SparkSession.builder().appName("cleaningData").getOrCreate();
		Dataset<Row> linesOtp = jss.read().format("com.databricks.spark.avro").load(args[0]);
		linesOtp.printSchema();
		linesOtp.na().fill("null") //
				.na().replace("CarrierDelay", Collections.singletonMap("NA", "0")) //
				.write().parquet(args[1]);
		// PLANEDATE files
		Dataset<Row> linesPlaneDate = jss.read().format("com.databricks.spark.avro").load(args[2]);
		linesPlaneDate.na().drop(2) //
				.write().parquet(args[3]);

		// carr files
		Dataset<Row> linesCarr = jss.read().format("com.databricks.spark.avro").load(args[4]);
		linesCarr.write().parquet(args[5]);
		// airports
		Dataset<Row> linesAirports = jss.read().format("com.databricks.spark.avro").load(args[6]);
		linesAirports.write().parquet(args[7]);

	}

}
