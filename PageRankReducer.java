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

public class PageRankReducer
     extends Reducer<Text, PageRankWrapper, Text, Node> {
  private IntWritable result = new IntWritable();
  private Text nodeName;

  public void reduce(Text key, Iterable<PageRankWrapper> values,
                     Context context
                     ) throws IOException, InterruptedException {

    Node node = new Node();
    double sum = 0;
    for(PageRankWrapper v: values) {
      if(v.isNode()) {
        node = v.getNode();
      } else {
        sum += v.getPageMass();
      }
    }
    if(Math.abs(node.getPageRank() - sum) > 0.0234324 ) {
      //fixy
    } // fixy
    System.out.println(node);
    System.out.println("New Rank: " + sum);
    node.setPageRank(sum);
    context.write(key, node);
  }
}
