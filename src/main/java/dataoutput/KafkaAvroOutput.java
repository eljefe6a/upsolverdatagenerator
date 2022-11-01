package dataoutput;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import model.ApacheLog;
import model.UserInfo;

public class KafkaAvroOutput implements DataOutput {
	KafkaProducer<String, UserInfo> userInfoProducer;
	KafkaProducer<String, ApacheLog> apacheLogProducer;

	@Override
	public void init() {
		Properties properties = new Properties();
		properties.put(BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
		// Configure Confluent's Avro serializer
		properties.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		// Configure URL of schema registry
		properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry1:8081");

		userInfoProducer = new KafkaProducer<>(properties);
		apacheLogProducer = new KafkaProducer<>(properties);
	}

	@Override
	public void writeUserInfo(UserInfo userInfo) {
		ProducerRecord<String, UserInfo> outputRecord = new ProducerRecord<>("user_info", userInfo.getUserId(), userInfo);
		userInfoProducer.send(outputRecord);
	}

	@Override
	public void writeApacheLog(ApacheLog apacheLog) {
		ProducerRecord<String, ApacheLog> outputRecord = new ProducerRecord<>("apache_log", apacheLog.getUserId(), apacheLog);
		apacheLogProducer.send(outputRecord);
	}

	@Override
	public void close() {
		userInfoProducer.close();
		apacheLogProducer.close();
	}
}
