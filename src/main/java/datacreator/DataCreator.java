package datacreator;

import java.util.HashMap;

import model.ApacheLog;
import model.UserInfo;
import scala.Tuple2;

/**
 * Interface for creating randomly UserInfo and ApacheLog objects
 */
public interface DataCreator {
	/** Initialize the DataCreator */
	public void init();

	/** Randomly create a Tuple of UserInfo and ApacheLog objects */
	public Tuple2<UserInfo, ApacheLog> create();

	/** Get the HashMap of user ID to the UserInfo object */
	public HashMap<String, UserInfo> getUserIdToUserInfo();
}
