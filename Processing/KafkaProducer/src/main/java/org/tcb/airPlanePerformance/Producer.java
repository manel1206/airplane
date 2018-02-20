package org.tcb.airPlanePerformance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


public class Producer {
	
	public static Map<String, Object> configKafkaSpark() {

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcb1.server.hdp.com:6667");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		return kafkaParams;
	}
	
	

	public static void main(String[] args) throws InterruptedException {

		TopicWriter topic_1 = new TopicWriter("M.Airports",
				"/home/manel/Desktop/hadoopTest/air plane performance /data/airports.csv");
		TopicWriter topic_2 = new TopicWriter("M.Carriers",
				"/home/manel/Desktop/hadoopTest/air plane performance /data/carriers.csv");
		TopicWriter topic_3 = new TopicWriter("M.Planedate",
				"/home/manel/Desktop/hadoopTest/air plane performance /data/plane-data (1).csv");
		TopicWriter topic_4 = new TopicWriter("M.OTP",
				"/home/manel/Desktop/hadoopTest/air plane performance /data/2008.csv",
				"/home/manel/Desktop/hadoopTest/air plane performance /data/2007.csv");

		Thread thread_1 = new Thread(topic_1);
		thread_1.start();
		System.out.println("**********");
		Thread thread_2 = new Thread(topic_2);
		thread_2.start();
		Thread thread_3 = new Thread(topic_3);
		thread_3.start();
		Thread thread_4 = new Thread(topic_4);
		thread_4.start();
		List<String> topics = new ArrayList<>();
		topics = Arrays.asList("M.Carriers", "M.OTP");
		
		

	}
}
