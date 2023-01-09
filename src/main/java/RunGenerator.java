import datacreator.DataCreator;
import datacreator.FakerCreator;
import dataoutput.AvroFileOutput;
import dataoutput.DataOutput;
import dataoutput.KafkaAvroBinaryOutput;
import dataoutput.KafkaAvroJSONOutput;
import model.ApacheLog;
import model.UserInfo;
import scala.Tuple2;

public class RunGenerator {
	public static void main(String[] args) {
		DataCreator dataCreator = new FakerCreator();
		dataCreator.init();
		
		DataOutput output = new KafkaAvroJSONOutput();
		//DataOutput output = new KafkaAvroBinaryOutput();
		//DataOutput output = new AvroFileOutput();
		output.init();
		
		// Write out initial UserInfos
		for (UserInfo userInfo : dataCreator.getUserIdToUserInfo().values()) {
			output.writeUserInfo(userInfo);
		}
		
		System.out.println("Wrote initial UserInfos");
		
		// Write out ApacheLogs and UserInfos
		for (int i = 0; i < 100_000; i++) {
			Tuple2<UserInfo, ApacheLog> create = dataCreator.create();
			
			// TODO: Randomize which is written first?
			output.writeApacheLog(create._2());
			
			// UserInfo is a brand new one. Write it out.
			if (create._1() != null) {
				output.writeUserInfo(create._1());
			}
		}
		
		System.out.println("Wrote out ApacheLogs and UserInfos");
		
		output.close();
	}
}
