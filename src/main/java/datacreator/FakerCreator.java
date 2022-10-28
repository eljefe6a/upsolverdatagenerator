package datacreator;

import java.util.HashMap;
import java.util.Random;

import model.ApacheLog;
import model.HTTPMethods;
import model.UserInfo;
import net.datafaker.Faker;
import scala.Tuple2;

public class FakerCreator implements DataCreator {
	HashMap<String, UserInfo> userIdToUserInfo = new HashMap<>();
	
	Faker faker;
	
	Random random;
	
	@Override
	public void init() {
		faker = new Faker();
		
		random = new Random();
		
		// Seed a certain number of existing UserInfo records
		for (int i = 0; i < 1000; i++) {
			UserInfo userInfo = createUserInfo();
			userIdToUserInfo.put(userInfo.getUserId(), userInfo);
		}
	}

	@Override
	public Tuple2<UserInfo, ApacheLog> create() {
		String userId;
		UserInfo userInfo;
		
		if (random.nextInt(10) > 8) {
			// Create a new UserInfo and add it list of other fake users
			userInfo = createUserInfo();
			userIdToUserInfo.put(userInfo.getUserId(), userInfo);
			
			userId = userInfo.getUserId();
		} else {
			// Randomly select a UserInfo
			UserInfo randomUserInfo = userIdToUserInfo.values().toArray(new UserInfo[0])[random.nextInt(userIdToUserInfo.size())];
			userId = randomUserInfo.getUserId();
			
			// Send null UserInfo since this isn't a new one
			userInfo = null;
		}
		
		ApacheLog apacheLog = createApacheLog(userId);
		
		return Tuple2.apply(userInfo, apacheLog);
	}
	
	private UserInfo createUserInfo() {
		UserInfo userInfo = UserInfo.newBuilder().build();
		
		userInfo.setUserId(faker.name().username());
		userInfo.setFirstName(faker.name().firstName());
		userInfo.setLastName(faker.name().lastName());
		
		userInfo.setUserId(faker.name().username());
		
		userInfo.setAddress(faker.address().fullAddress());
		userInfo.setPhone(faker.phoneNumber().phoneNumber());
		
		userInfo.setPassword(faker.internet().password());
		userInfo.setSubscription(faker.subscription().subscriptionTerms());
		userInfo.setCreditCard(faker.business().creditCardNumber());
		
		return userInfo;
	}
	
	private ApacheLog createApacheLog(String userId) {
		ApacheLog apacheLog = ApacheLog.newBuilder().build();
		
		// TODO: Make IP and browser client consistent?
		apacheLog.setHost(faker.internet().ipV4Address());
		apacheLog.setUserId(userId);
		
		// TODO
		//apacheLog.setTimestamp(System.currentTimeMillis());
		
		apacheLog.setHttpMethod(HTTPMethods.GET);
		apacheLog.setUrl(faker.internet().url());
		apacheLog.setStatusCode("200");
		apacheLog.setBrowserClient(faker.internet().userAgentAny());
		
		return apacheLog;
	}

	@Override
	public HashMap<String, UserInfo> getUserIdToUserInfo() {
		return userIdToUserInfo;
	}
}
