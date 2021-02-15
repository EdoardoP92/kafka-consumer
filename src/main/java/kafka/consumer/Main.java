package kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {

	public static void main(String[] args) {
		
		/**
		 * args[0] = bootstrap.servers
		 * args[1] = topic
		 */
		
		Logger log = Logger.getLogger(Main.class.getSimpleName());
		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", args[0]);
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(args[1]));
		
		ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(5));
		
		for(ConsumerRecord<String,String> record : records) {
			log.info("********** MESSAGE RECEIVED: **********\n"+record.value());
		}
		consumer.close();
	}
}
