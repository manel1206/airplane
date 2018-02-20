package org.tcb.airPlanePerformance;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TopicWriter implements Runnable {
 
	private String[] paths ;
	private String TopicName ;
	
	
	
	
	public TopicWriter(String topicName, String... path) {
		
		this.paths = path;
		TopicName = topicName;
	}

	public TopicWriter() {
		
	}

    public String[] getPath() {
		return paths;
	}
    public void setPath(String[] path) {
		this.paths = path;
	}
    public String getTopicName() {
		return TopicName;
	}
    public void setTopicName(String topicName) {
		TopicName = topicName;
	}
    
    public static Properties config() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "tcb1.server.hdp.com:6667");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		return props;
	}
    
   
    
     public void run() {
    	 Properties prop = config();
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
	        

	        
	        Arrays.asList(paths).forEach(path -> {
	        	String currentLine = null;
		        BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(path));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        
	        try {
				while((currentLine = reader.readLine()) != null){
				    System.out.println("---------------------");
				    System.out.println(currentLine);
				    System.out.println("---------------------");
				    ProducerRecord<String, String>  message = new ProducerRecord<String, String>(TopicName, currentLine);
				    producer.send(message);
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        });
	        producer.close();   
	    }  
		
	}



