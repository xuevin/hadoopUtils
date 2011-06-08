package uk.ac.ebi.fgpt.hadoopUtils.pca.distributed;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

/**
 * This application creates a SequenceFile with <IntWritable,VectorWritable> keyvalue pairs. arg[0] is the
 * input file arg[1] is the output file
 * 
 * @author vincent@ebi.ac.uk
 * 
 */
public class CreateNormalizedVectorFromTxt_MapRed extends Configured implements Tool {
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CreateNormalizedVectorFromTxt_MapRed(), args);
    System.exit(res);
  }
  
  public static class Map extends MapReduceBase implements
      Mapper<LongWritable,Text,IntWritable,VectorWritable> {
    
    public void map(LongWritable key,
                    Text value,
                    OutputCollector<IntWritable,VectorWritable> output,
                    Reporter reporter) throws IOException {
      String line = value.toString();
      output.collect(new IntWritable((int) key.get()), new VectorWritable(StringToVector.normalize(line)));
    }
    
  }
  
  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    
    public void reduce(IntWritable key,
                       Iterator<VectorWritable> value,
                       OutputCollector<IntWritable,VectorWritable> output,
                       Reporter reporter) throws IOException {
      
      output.collect(key, value.next());
      if (value.hasNext()) {
        reporter.setStatus("Multiple Copies of key found in reduce");
      }
    }
    
  }
  
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), CreateNormalizedVectorFromTxt_MapRed.class);
    conf.setJobName("Write Vector SequenceFile");
    
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(VectorWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);
    return 0;
  }
  
}
