package uk.ac.ebi.fgpt.hadoopUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public abstract class HadoopTestCase {

	protected final Path getTestTempDirPath(Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
		Path testTempDirPath = fs.makeQualified(new Path("/tmp/hadoopUtils-"
				+ getClass().getSimpleName() + '-' + simpleRandomLong));
		if (!fs.mkdirs(testTempDirPath)) {
			throw new IOException("Could not create " + testTempDirPath);
		}
		fs.deleteOnExit(testTempDirPath);
		return testTempDirPath;
	}

	protected static void createSequenceFileFromData(Configuration conf,
			Path inputPath, String[][] data) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				inputPath, IntWritable.class, VectorWritable.class);
		int rowIndex = 0;
		for (String[] aData : data) {
			int index = 0;
			Vector vector = new SequentialAccessSparseVector(aData.length);
			for (String bData : aData) {
				vector.set(index, Double.parseDouble(bData));
				index++;
			}
			writer
					.append(new IntWritable(rowIndex), new VectorWritable(
							vector));
			rowIndex++;
		}
		writer.close();
	}

	protected static void createTsvFilesFromArrays(Configuration conf,
			Path inputDir, String[][] data) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		OutputStreamWriter osw = new OutputStreamWriter(fs.create(new Path(
				inputDir.toString())));
		for (String[] aData : data) {
			osw.write(aData[0] + '\t' + aData[1] + '\t' + aData[2] + '\n');
		}
		osw.close();
	}
}
