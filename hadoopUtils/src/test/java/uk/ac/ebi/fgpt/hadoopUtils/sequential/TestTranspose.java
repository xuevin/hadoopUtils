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

public class TestTranspose extends HadoopTestCase {
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
	public void testTranspose() throws IOException {
		Transpose.run(inputPath.toString(), outputPath.toString(), 2, 3);
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("There are not 3 rows", 3, vectorMap.size());
		assertEquals("The first row index is 0", 0, vectorMap.firstKey().get());
	}

}
