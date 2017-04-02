import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.lang.ClassNotFoundException;
import java.lang.InterruptedException;
import java.util.Map;
import java.util.HashMap;

public class PageRank {

  private static void writeStringToFile(String s, FSDataOutputStream out) throws IOException {
    out.writeChars(s);
    out.writeChars(" ");
  }

  private static double makeInitalInputFile(FileSystem fs, Path inputFile, Path tempDir) throws IOException{
    //read the file
    FSDataInputStream inFile = fs.open(inputFile);
    int numNodes = Integer.parseInt(inFile.readLine().trim());
    double convergence =  Double.parseDouble(inFile.readLine().trim());

    boolean canRead = true;
    String line;
    String[] tokens;
    Map<String, List<String>> temp = new HashMap<>();

    while(canRead){
      try {
        line = inFile.readLine();
        if(line == null) {
          break;
        }
        tokens = line.split(" ");
        if(temp.get(tokens[0]) == null){
          temp.put(tokens[0], new ArrayList(numNodes));
        }
        temp.get(tokens[0]).add(tokens[1]);
      } catch (EOFException e) {
        canRead = false;
      }
    }

    FSDataOutputStream out = fs.create(new Path(tempDir, "input"), true);
    for(Map.Entry<String, List<String>> entry : temp.entrySet()){
      writeStringToFile(entry.getKey(), out);
      writeStringToFile(entry.getKey(), out);
      writeStringToFile(Double.toString(1.0 / numNodes), out);
      for(String outlink : entry.getValue()) {
        writeStringToFile(outlink, out);
      }
      out.writeChar('\n');
    }
    return convergence;
  }


  public static void runPageRank(Configuration conf, Path inputFile, Path tmpDir)
        throws IOException, InterruptedException, ClassNotFoundException {
    final FileSystem fs = FileSystem.get(conf);

    //setup input/output directories
    final Path inDir = new Path(tmpDir, "in");
    final Path outDir = new Path(tmpDir, "out");
    if (fs.exists(tmpDir)) {
      throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
          + " already exists.  Please remove it first.");
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Cannot create input directory " + inDir);
    }

    //read the file
    double convergence = makeInitalInputFile(fs, inputFile, inDir);
    boolean needsNewRun = true;
    int currentIteration = 0;
    conf.setDouble("convergence", convergence);


    // //start the jobs loop
    // while(needsNewRun) {
    //   // setup temp directories
    Job job = setUpJob(conf, currentIteration, inDir, outDir);
    System.out.println("Starting JOb");
    job.waitForCompletion(true);
    //   ++currentIteration;
    //   needsNewRun = job.getCounters().findCounter(NEEDS_RUN).getValue() > 0;
    // }
  }



  public static Job setUpJob(Configuration conf, int iteration, Path inputFile, Path outputFile)
    throws IOException {
      Job job = Job.getInstance(conf, "pageRank: " + iteration);
      // setUpJob(job, args[0], args[1]);
      job.setJarByClass(PageRank.class);
      job.setInputFormatClass(PageRankInputFormat.class);

      job.setMapperClass(PageRankMapper.class);
      job.setReducerClass(PageRankReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(PageRankWrapper.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Node.class);

      // turn off speculative execution, because DFS doesn't handle
      // multiple writers to the same file.
      job.setSpeculativeExecution(false);

      System.out.println(inputFile.toString());
      FileInputFormat.addInputPath(job, inputFile);
      FileOutputFormat.setOutputPath(job, outputFile);

      return job;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    conf.set("mapreduce.output.textoutputformat.separator", " ");
    final long startTime = System.currentTimeMillis();
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    final double duration = (System.currentTimeMillis() - startTime)/1000.0;
    runPageRank(conf, new Path(args[0]), new Path(args[1]));
    System.out.println("Job Finished in " + duration + " seconds");
  }
}
