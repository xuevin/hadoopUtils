package uk.ac.ebi.fgpt.hadoopUtils.pca.sequential;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.VectorWritable;

import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

public class CreateVectorFromTxt extends SequentialTool {

	public static void main(String[] args) throws IOException {

		// Create Options
		Options cliOptions = new Options();
		Option input = OptionBuilder.withArgName("input.tsv").hasArg()
				.isRequired().withDescription("use given file as input")
				.withLongOpt("input").create("i");
		Option output = OptionBuilder.withArgName("output.seqFile").hasArg()
				.isRequired().withLongOpt("output").withDescription(
						"use given file as output").create("o");
		Option headerRows = OptionBuilder.withArgName("num").hasArg()
				.withLongOpt("headerRow").withDescription(
						"The given input has <num> header rows").create("hr");
		Option headerColumns = OptionBuilder.withArgName("num").hasArg()
				.withLongOpt("headerColumn").withDescription(
						"The given input has <num> header columns")
				.create("hc");

		// Add Options
		cliOptions.addOption(input);
		cliOptions.addOption(output);
		cliOptions.addOption(headerRows);
		cliOptions.addOption(headerColumns);

		HelpFormatter formatter = new HelpFormatter();

		// Try to parse options
		CommandLineParser parser = new PosixParser();

		if (args.length <= 1) {
			formatter.printHelp("Create Vector From Txt", cliOptions, true);
			return;
		}

		try {
			CommandLine cmd = parser.parse(cliOptions, args, true);
			String pathToInput = cmd.getOptionValue("i");
			String pathToOutput = cmd.getOptionValue("o");
			int numHeaderColumns = cmd.hasOption("hc") ? Integer.parseInt(cmd
					.getOptionValue("hc")) : 0;
			int numHeaderRows = cmd.hasOption("hr") ? Integer.parseInt(cmd
					.getOptionValue("hr")) : 0;
			run(pathToInput, pathToOutput, numHeaderRows, numHeaderColumns);

		} catch (ParseException e) {
			formatter.printHelp("Create Vector From Txt", cliOptions, true);
		}

	}

	public static void run(String stringToInput, String stringToOutput,
			int headerRows, int headerColumns) throws IOException {

		setup(stringToInput, stringToOutput);

		// Open a file for reading
		FSDataInputStream in = fs.open(inputPath);

		try {
			// Create Writer
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, config,
					outputPath, IntWritable.class, VectorWritable.class);

			// Create Buffered Reader (which reads from input stream)
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					in, "UTF-8"));

			int rowsToSkip = 0;

			int index = 0;
			String line;
			while ((line = reader.readLine()) != null) {
				if (rowsToSkip < headerRows) {
					rowsToSkip++;
				} else {
					writer.append(new IntWritable(index), new VectorWritable(
							StringToVector.convert(line, headerColumns)));
					index++;
				}
			}
			writer.close();
		} finally {
			in.close();
		}

	}
}
