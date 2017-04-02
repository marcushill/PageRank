import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.LineRecordReader;
import java.util.ArrayList;
import java.util.List;

public class PageRankRecordReader extends RecordReader<Text, Node> {

  private Text key;
  private Node value;

  private LineRecordReader reader = new LineRecordReader();

  public PageRankRecordReader()

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    reader.close();
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return key;
  }

  @Override
  public Node getCurrentValue() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return reader.getProgress();
  }

  @Override
  public void initialize(InputSplit is, TaskAttemptContext tac)
      throws IOException, InterruptedException {
    System.out.println("Initializing");
    reader.initialize((FileSplit)is, tac);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    boolean gotNextKeyValue = reader.nextKeyValue();
    System.out.println("AHDAD: " + gotNextKeyValue);
    if(gotNextKeyValue) {
      if(key==null){
        key = new Text();
      }

      if(value == null){
        value = new Node();
      }

      Text line = reader.getCurrentValue();
      String[] tokens = line.toString().trim().split(" ");
      key.set(tokens[0]);
      double pageRank = Double.parseDouble(tokens[1]);
      List<String> outlinks = new ArrayList<>();

      for (int i = 2; i < tokens.length; i++){
        outlinks.add(tokens[i]);
      }

      value.setPageRank(pageRank);
      value.setOutlinks(outlinks);
      value.setNodeId(tokens[0]);
      System.out.println("Presenting: ");
      System.out.println(key);
      System.out.println(value);
    } else {
      key = null;
      value = null;
    }
    return gotNextKeyValue;
  }
}
