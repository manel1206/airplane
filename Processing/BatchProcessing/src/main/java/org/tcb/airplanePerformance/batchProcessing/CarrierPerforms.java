package org.tcb.airplanePerformance.batchProcessing;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class CarrierPerforms {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("carrierPerform").getOrCreate();
		Dataset<Row> otp = spark.read().parquet(args[0]);

		Dataset<Row> otp2 = otp.select(functions.col("UniqueCarrier"),
		 functions.col("ArrDelay").cast("double").as("ArrDelay"))
		 .groupBy("UniqueCarrier").avg("ArrDelay").as("avgarr");
		otp2.show();
		otp2.groupBy("UniqueCarrier").min("avg(ArrDelay)").show();
//		otp.select(functions.col("UniqueCarrier"), functions.col("ArrDelay").cast("double").as("ArrDelay"),
//				functions.col("DepDelay").cast("double").as("ArrDelay"))
//				.agg(functions.col("UniqueCarrier"), functions.col("ArrDelay")).show();

	}

}
