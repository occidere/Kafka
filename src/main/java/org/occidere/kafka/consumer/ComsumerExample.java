package org.occidere.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ComsumerExample {
	private static final String SERVERS = "ncloud-centos-occidere:9092,ncloud-centos-occidere000:9092,ncloud-centos-sj9401h:9092";
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		//props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // --from-beginning
		
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(()-> {
			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList("test"));
			
			ConsumerRecords<String, String> records = consumer.poll(500);
			for(ConsumerRecord<String, String> record : records) {
				switch(record.topic()) {
					case "test":
						System.out.println(record);
						//System.out.printf("offset(%3d), partition(%3d)\t%s\n", record.offset(), record.partition(), record.value());
						break;
					default:
						throw new IllegalStateException("get message on topic " + record.topic());
				}
			}
			
			consumer.close();
		}, 0, 10, TimeUnit.SECONDS);
	}
}
