package uk.ac.ebi.fgpt.hadoopUtils.sequential;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Transpose extends SequentialTool {
	public static void main(String[] args) throws IOException {

		// Create Options
		Options cliOptions = new Options();
		Option input = OptionBuilder.withArgName("input.seqFile").hasArg()
				.isRequired().withDescription("use given file as input")
				.withLongOpt("input").create("i");
		Option output = OptionBuilder.withArgName("output.seqFile").hasArg()
				.isRequired().withLongOpt("output").withDescription(
						"use given file as output").create("o");
		Option rows = OptionBuilder.withArgName("num").hasArg()
				.isRequired().withLongOpt("rows").withDescription(
						"The given input has <num> rows").create("r");
		Option columns = OptionBuilder.withArgName("num").hasArg()
				.isRequired().withLongOpt("columns").withDescription(
						"The given input has <num> columns").create("c");

		// Add Options
		cliOptions.addOption(input);
		cliOptions.addOption(output);
		cliOptions.addOption(rows);
		cliOptions.addOption(columns);

		HelpFormatter formatter = new HelpFormatter();

		// Try to parse options
		CommandLineParser parser = new PosixParser();

		if (args.length <= 1) {
			formatter.printHelp("transpose", cliOptions, true);
			return;
		}

		try {
			CommandLine cmd = parser.parse(cliOptions, args, true);
			String pathToInput = cmd.getOptionValue("i");
			String pathToOutput = cmd.getOptionValue("o");
			int numColumns = Integer.parseInt(cmd.getOptionValue("c"));
			int numRows = Integer.parseInt(cmd.getOptionValue("r"));
			run(pathToInput, pathToOutput, numRows, numColumns);

		} catch (ParseException e) {
			formatter.printHelp("transpose", cliOptions, true);
		}
	}

	public static void run(String stringToInput, String stringToOutput,
			int numRows, int numColumns) throws IOException {

		setup(stringToInput, stringToOutput);

		// Create Writer
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, config,
				outputPath, IntWritable.class, VectorWritable.class);
		// Create Reader
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath,
				config);

		// This is a very inefficient algorithm that iterates through a sequence
		// file
		// as many times as there are columns
		// For each iteration, it writes out the tranpose of the column

		try {
			// Make the single objects that will be used over and over again.
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass()
					.newInstance();

			for (int i = 0; i < numColumns; i++) {
				System.out.println("Status:" + i + "/" + numColumns);
				Vector tranposedColumn = new SequentialAccessSparseVector(
						numRows);

				while (reader.next(key, value)) {
					tranposedColumn.set(key.get(), value.get().get(i));
				}
				writer.append(new IntWritable(i), new VectorWritable(
						tranposedColumn));

				// Restart the reader
				reader = new SequenceFile.Reader(fs, inputPath, config);
			}
			System.out.println("Complete!");

			writer.close();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

	}

}
