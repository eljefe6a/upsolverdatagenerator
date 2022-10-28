import datacreator.DataCreator;
import datacreator.FakerCreator;
import dataoutput.DataOutput;
import dataoutput.KafkaAvroOutput;
import model.ApacheLog;
import model.UserInfo;
import scala.Tuple2;

public class RunGenerator {
	public static void main(String[] args) {
		DataCreator dataCreator = new FakerCreator();
		dataCreator.init();
		
		DataOutput output = new KafkaAvroOutput();
		
		// Write out initial UserInfos
		for (UserInfo userInfo : dataCreator.getUserIdToUserInfo().values()) {
			output.writeUserInfo(userInfo);
		}
		
		// Write out ApacheLogs and UserInfos
		for (int i = 0; i < 1000; i++) {
			Tuple2<UserInfo, ApacheLog> create = dataCreator.create();
			
			// Randomize which is written first?
			output.writeApacheLog(create._2());
			
			// UserInfo is reused and written before
			if (output != null) {
				output.writeUserInfo(create._1());
			}
		}
		
		output.close();
	}
}
