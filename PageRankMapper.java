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
public class PageRankMapper extends Mapper<Text, Node, Text, PageRankWrapper>{

  // private IntWritable count;
  // private static final Text outKey = new Text("R");

  public void map(Text key, Node value, Context context
                  ) throws IOException, InterruptedException {

    System.out.println("ARALRHAIOFLDHFL:KSDHFLK:SDFSDFJ: " + key);
    context.write(key, new PageRankWrapper(value));
    double pageMass = value.getPageRank() / value.getOutlinks().size();

    for(String node : value.getOutlinks()){
      context.write(new Text(node), new PageRankWrapper(pageMass));
    }

  }
}
