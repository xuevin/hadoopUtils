package uk.ac.ebi.fgpt.hadoopUtils.sequential;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SequentialTool {
	protected static Path inputPath;
	protected static Path outputPath;
	protected static Configuration config;
	protected static FileSystem fs;

	protected static void setup(String stringToInput, String stringToOutput)
			throws IOException {

		config = new Configuration();
		fs = FileSystem.get(config);
		inputPath = new Path(stringToInput);
		outputPath = new Path(stringToOutput);

		if (!fs.exists(inputPath))
			printAndExit("Input file not found");
		if (!fs.isFile(inputPath))
			printAndExit("Input should be a file");
		if (fs.exists(outputPath))
			printAndExit("Output already exists");
	}

	protected static void setup(String pathToInput) throws IOException {
		config = new Configuration();
		fs = FileSystem.get(config);
		inputPath = new Path(pathToInput);

		if (!fs.exists(inputPath))
			printAndExit("Input file not found");
		if (!fs.isFile(inputPath))
			printAndExit("Input should be a file");
	}

	protected static void printAndExit(String string) {
		System.out.println(string);
		System.exit(0);
	}

}
