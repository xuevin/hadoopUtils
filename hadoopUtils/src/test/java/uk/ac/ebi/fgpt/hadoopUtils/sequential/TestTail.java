package uk.ac.ebi.fgpt.hadoopUtils.sequential;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.ReadVectors;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.Tail;

public class TestTail extends HadoopTestCase {
	private String[][] data = new String[][] { { "2", "4", "6" },
			{ "8", "10", "12" } };
	private Path tmpDir;
	private Path inputPath;
	private Configuration conf;
	private Path outputPath;
	private TreeMap<IntWritable, VectorWritable> vectorMap;

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();

		// create
		tmpDir = getTestTempDirPath(conf);
		inputPath = new Path(tmpDir, "inputSeqFile");
		outputPath = new Path(tmpDir, "outputSeqFile");

		createSequenceFileFromData(conf, inputPath, data);

		// Make new vectorMap
		vectorMap = new TreeMap<IntWritable, VectorWritable>();
	}

	@Test
	public void testTail_2() throws IOException {
		System.out.println("A 2x3 Matrix is Below");
		Tail.run(inputPath.toString(), outputPath.toString(), 2);
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("Number of rows is not equal to 2", 2, vectorMap.size());
	}

	@Test
	public void testTail_1() throws IOException {
		System.out.println("A 1x3 Matrix is Below");
		Tail.run(inputPath.toString(), outputPath.toString(), 1);
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("Number of rows is not equal to 1", 1, vectorMap.size());
	}

	@Test
	public void taestTail_ExceedsSize() throws IOException {
		System.out
				.println("Exceeds Size, but still prints out all that it did find");
		Tail.run(inputPath.toString(), outputPath.toString(), 3);
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("Number of rows is not equal to 2", 2, vectorMap.size());
	}
}
