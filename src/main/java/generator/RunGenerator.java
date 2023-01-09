package generator;

import datacreator.DataCreator;
import datacreator.FakerCreator;
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

public class RunGenerator {
	public static void main(String[] args) {
		Namespace ns = parseCommandLineArgs(args);

		DataCreator dataCreator = new FakerCreator();
		dataCreator.init();

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

	private static Namespace parseCommandLineArgs(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("RunGenerator").build().defaultHelp(true)
				.description("Creates plausible fake data and outputs in a variety of formats.");
		parser.addArgument("-o", "--output").choices("fileavro", "kafkajson", "kafkaavro").setDefault("fileavro")
				.help("Specify the output format and technology");
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
