package org.tcb.airplanePerformance.batchProcessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ipc.StandbyException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.spark.SparkConf;

public class KafkaSparkStream {

	public static Map<String, Object> configKafkaSpark() {

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcb1.server.hdp.com:6667,tcb2.server.hdp.com:6667");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "mnl");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return kafkaParams;
	}

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 3) {
			System.err.print("params?!");
			System.exit(1);
		}
		String destFolder = args[0];
		String clusterName = args[1];
		
		List<String> topics = new ArrayList<>();
		topics = Arrays.asList("M.OTP", "M.Carriers");

		Map<String, Object> KafkaParm = configKafkaSpark();

		SparkConf sparkConf = new SparkConf().setAppName("consumeTopicsWithSpark");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		
			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, KafkaParm));
				
			JavaDStream<String> lines = stream.map(kafkaRecord -> kafkaRecord.value());
			//lines.print();
			lines.dstream().saveAsTextFiles("hdfs://"+clusterName , "");

			jssc.start();
			jssc.awaitTermination();
			jssc.stop();

	}

}
