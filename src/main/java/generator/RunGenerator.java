package generator;

import datacreator.DataCreator;
import datacreator.UserInfoAndApacheLogFakerCreator;
import dataoutput.AvroFileOutput;
import dataoutput.DataOutput;
import dataoutput.KafkaAvroBinaryOutput;
import dataoutput.KafkaAvroJSONOutput;
import model.ApacheLog;
import model.UserInfo;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

/**
 * Program to create randomly generated user and Apache log data. The data gets
 * written to a user-chosen output.
 *
 */
public class RunGenerator {
	public static void main(String[] args) {
		Namespace ns = parseCommandLineArgs(args);

		DataCreator dataCreator = new UserInfoAndApacheLogFakerCreator();
		dataCreator.init();

		DataOutput output = initDataOutput(ns);

		// Write out initial UserInfos
		for (UserInfo userInfo : dataCreator.getUserIdToUserInfo().values()) {
			output.writeUserInfo(userInfo);
		}

		System.out.println("Wrote initial UserInfos");

		writeMessages(ns, dataCreator, output);

		output.close();
	}

	/**
	 * Writes out the UserInfo and ApacheLog objects to the DataOutput
	 * 
	 * @param ns          The parsed command line
	 * @param dataCreator The data creator to randomly create the UserInfo and
	 *                    ApacheLog
	 * @param output      The DataOutput to write to
	 */
	private static void writeMessages(Namespace ns, DataCreator dataCreator, DataOutput output) {
		long numberOfMessages = ns.getLong("number");

		System.out.println("Writing " + numberOfMessages + " Apache log messages.");

		// Write out ApacheLogs and UserInfos
		for (long i = 0; i < numberOfMessages; i++) {
			Tuple2<UserInfo, ApacheLog> create = dataCreator.create();

			// TODO: Randomize which is written first?
			output.writeApacheLog(create._2());

			// UserInfo is a brand new one. Write it out.
			if (create._1() != null) {
				output.writeUserInfo(create._1());
			}
		}

		System.out.println("Wrote out ApacheLogs and UserInfos");
	}

	/**
	 * Figures out and initializes the DataOutput type.
	 * 
	 * @param ns The parsed namespace
	 * @return The chosen DataOutput type
	 */
	private static DataOutput initDataOutput(Namespace ns) {
		DataOutput output = null;

		String outputType = ns.getString("output");

		// Choose and initialize the user's output of choice
		if (outputType.equals("fileavro")) {
			output = new AvroFileOutput();
		} else if (outputType.equals("kafkajson")) {
			output = new KafkaAvroJSONOutput();
		} else if (outputType.equals("kafkaavro")) {
			output = new KafkaAvroBinaryOutput();
		}
		
		System.out.println("Writing out with:" + outputType);

		output.init();

		return output;
	}

	/**
	 * Parses out the command line arguments. Exits if incorrect arguments are
	 * given.
	 * 
	 * @param args The command line arguments
	 * @return The parsed arguments.
	 */
	private static Namespace parseCommandLineArgs(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("RunGenerator").build().defaultHelp(true)
				.description("Creates plausible fake data and outputs in a variety of formats.");
		parser.addArgument("-o", "--output").choices("fileavro", "kafkajson", "kafkaavro").setDefault("fileavro")
				.help("Specify the output format and technology");
		parser.addArgument("-n", "--number").type(Long.class).help("The number of apache log messages to send.")
				.setDefault(100_000L);
		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}
		return ns;
	}
}
