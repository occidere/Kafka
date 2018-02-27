package org.occidere.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerExample {
	private static final String SERVERS = "ncloud-centos-occidere:9092,ncloud-centos-occidere000:9092,ncloud-centos-sj9401h:9092";
	
	public static void main(String args[]) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
	
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		String timestamp = new SimpleDateFormat("YYYY-MM-DD_HH:mm:ss,S").format(new Date());
		String msg = "Producer test at " + timestamp;
		
		producer.send(new ProducerRecord<>("test", msg),
			(metadata, exception) -> {
				if(metadata != null) {
					System.out.printf("[%s] partition(%s), offset(%s)\n", timestamp, metadata.partition(), metadata.offset());
				}
				else {
					exception.printStackTrace();
				}
			});
		producer.close();
	}
}