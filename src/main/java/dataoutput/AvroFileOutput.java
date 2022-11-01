package dataoutput;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import model.ApacheLog;
import model.UserInfo;

public class AvroFileOutput implements DataOutput {
	DataFileWriter<UserInfo> userInfoFileWriter;
	DataFileWriter<ApacheLog> apacheLogFileWriter;
	
	@Override
	public void init() {
		try {
			DatumWriter<UserInfo> userInfoWriter = new SpecificDatumWriter<>(UserInfo.getClassSchema());
			userInfoFileWriter = new DataFileWriter<>(userInfoWriter);
		
			userInfoFileWriter.create(UserInfo.getClassSchema(), new File("userinfo.avro"));
		
			DatumWriter<ApacheLog> apacheLogWriter = new SpecificDatumWriter<>(ApacheLog.getClassSchema());
			apacheLogFileWriter = new DataFileWriter<>(apacheLogWriter);
			
			apacheLogFileWriter.create(ApacheLog.getClassSchema(), new File("apachelog.avro"));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	@Override
	public void writeUserInfo(UserInfo userInfo) {
		try {
			userInfoFileWriter.append(userInfo);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeApacheLog(ApacheLog apacheLog) {
		try {
			apacheLogFileWriter.append(apacheLog);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			userInfoFileWriter.close();
			apacheLogFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
