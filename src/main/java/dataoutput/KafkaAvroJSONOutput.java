package dataoutput;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import model.ApacheLog;
import model.UserInfo;

public class KafkaAvroJSONOutput implements DataOutput {
	KafkaProducer<String, String> userInfoProducer;
	KafkaProducer<String, String> apacheLogProducer;

	ByteArrayOutputStream userInfoBOS;
	Encoder userInfoEncoder;
	DatumWriter<UserInfo> userInfoWriter;

	ByteArrayOutputStream apacheLogBOS;
	Encoder apacheLogEncoder;
	DatumWriter<ApacheLog> apacheLogWriter;

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

			// Configure JSON/String serializer
			properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			userInfoProducer = new KafkaProducer<>(properties);
			apacheLogProducer = new KafkaProducer<>(properties);

			userInfoBOS = new ByteArrayOutputStream();
			userInfoEncoder = EncoderFactory.get().jsonEncoder(UserInfo.getClassSchema(), userInfoBOS);

			userInfoWriter = new SpecificDatumWriter<>();
			userInfoWriter.setSchema(UserInfo.getClassSchema());

			apacheLogBOS = new ByteArrayOutputStream();
			apacheLogEncoder = EncoderFactory.get().jsonEncoder(ApacheLog.getClassSchema(), apacheLogBOS);

			apacheLogWriter = new SpecificDatumWriter<>();
			apacheLogWriter.setSchema(ApacheLog.getClassSchema());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@Override
	public void writeUserInfo(UserInfo userInfo) {
		try {
			userInfoWriter.write(userInfo, userInfoEncoder);
			userInfoEncoder.flush();
			String userInfoJSON = new String(userInfoBOS.toByteArray(), StandardCharsets.UTF_8).strip();

			ProducerRecord<String, String> outputRecord = new ProducerRecord<>("user_info_json", userInfo.getUserId(),
					userInfoJSON);
			userInfoProducer.send(outputRecord);

			userInfoBOS.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeApacheLog(ApacheLog apacheLog) {
		try {
			apacheLogWriter.write(apacheLog, apacheLogEncoder);
			apacheLogEncoder.flush();
			String apacheLogJSON = new String(apacheLogBOS.toByteArray(), StandardCharsets.UTF_8).strip();

			ProducerRecord<String, String> outputRecord = new ProducerRecord<>("apache_log_json", apacheLog.getUserId(),
					apacheLogJSON);
			apacheLogProducer.send(outputRecord);

			apacheLogBOS.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() {
		userInfoProducer.close();
		apacheLogProducer.close();
	}
}
