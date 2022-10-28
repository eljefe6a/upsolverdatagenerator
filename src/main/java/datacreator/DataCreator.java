package datacreator;

import java.util.HashMap;

import model.ApacheLog;
import model.UserInfo;
import scala.Tuple2;

public interface DataCreator {
	public void init();
	public Tuple2<UserInfo, ApacheLog> create();
	public HashMap<String, UserInfo> getUserIdToUserInfo();
}
