package datacreator;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;

import model.ApacheLog;
import model.HTTPMethods;
import model.UserInfo;
import net.datafaker.Faker;
import scala.Tuple2;

/**
 * Creates random UserInfo and ApacheLog objects using the Faker library to
 * create plausible fake data.
 */
public class UserInfoAndApacheLogFakerCreator implements DataCreator {
	/** A map of user ID to UserInfo object */
	HashMap<String, UserInfo> userIdToUserInfo = new HashMap<>();

	/** The faker to generate plausible fake data */
	Faker faker;

	/** Random to make random decisions */
	Random random;

	/** The date time format for default Apache log date times */ 
	DateTimeFormatter apacheLogDTF;

	/** The gradually increasing date time that the Apache log timestamps use */
	ZonedDateTime logTime;

	@Override
	public void init() {
		faker = new Faker();

		apacheLogDTF = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

		logTime = ZonedDateTime.now();

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
			UserInfo randomUserInfo = userIdToUserInfo.values().toArray(new UserInfo[0])[random
					.nextInt(userIdToUserInfo.size())];
			userId = randomUserInfo.getUserId();

			// Send null UserInfo since this isn't a new one
			userInfo = null;
		}

		ApacheLog apacheLog = createApacheLog(userId);

		return Tuple2.apply(userInfo, apacheLog);
	}

	/**
	 * Creates a fake UserInfo object
	 * @return The fake UserInfo object
	 */
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

	/**
	 * Creates a fake ApacheLog object
	 * @return The fake ApacheLog object
	 */
	private ApacheLog createApacheLog(String userId) {
		ApacheLog apacheLog = ApacheLog.newBuilder().build();

		// TODO: Make IP and browser client consistent?
		apacheLog.setHost(faker.internet().ipV4Address());
		apacheLog.setUserId(userId);
		apacheLog.setTimestamp(apacheLogDTF.format(logTime));

		// Advance the log time a random amount of time
		logTime = logTime.plusSeconds(random.nextInt(15));

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
