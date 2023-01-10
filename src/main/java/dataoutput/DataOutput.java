package dataoutput;

import model.ApacheLog;
import model.UserInfo;

/**
 * Interface for the various ways to write out the UserInfo and ApacheLog
 * objects.
 *
 */
public interface DataOutput {
	/** Initializes the data output object */
	public void init();

	/** Writes out the UserInfo object */
	public void writeUserInfo(UserInfo userInfo);

	/** Writes out the ApacheLog object */ 
	public void writeApacheLog(ApacheLog apacheLog);

	/** Closes any resources opened by the writer */
	public void close();
}
