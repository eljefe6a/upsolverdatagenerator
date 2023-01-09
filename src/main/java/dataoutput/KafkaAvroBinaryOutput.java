package dataoutput;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import model.ApacheLog;
import model.UserInfo;

public class KafkaAvroBinaryOutput implements DataOutput {
	KafkaProducer<String, UserInfo> userInfoProducer;
	KafkaProducer<String, ApacheLog> apacheLogProducer;

	public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
	

	@Override
	public void init() {
		try {
			// Use the client.properties to set the various producer properties
			Properties properties = loadConfig("client.properties");
			
			// Configure Confluent's Avro serializer
			properties.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
			properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
			
			userInfoProducer = new KafkaProducer<>(properties);
			apacheLogProducer = new KafkaProducer<>(properties);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
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
