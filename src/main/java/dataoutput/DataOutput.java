package dataoutput;

import model.ApacheLog;
import model.UserInfo;

public interface DataOutput {
	public void writeUserInfo(UserInfo userInfo);
	public void writeApacheLog(ApacheLog apacheLog);
	public void close();
}
