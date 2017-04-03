import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.util.ArrayList;
import java.util.List;

public class PageRankRecordReader extends RecordReader<Text, Node> {

  private Text key;
  private Node value;

  private LineRecordReader reader = new LineRecordReader();

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Node getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }

  @Override
  public void initialize(InputSplit is, TaskAttemptContext tac)
      throws IOException, InterruptedException {
    reader.initialize(is, tac);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean gotNextKeyValue = reader.nextKeyValue();
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
      tokens[2] = tokens[2].trim().replaceAll("\0", "");

      double pageRank = Double.valueOf(tokens[2]);
      List<String> outlinks = new ArrayList<>();

      for (int i = 3; i < tokens.length; i++){
        outlinks.add(tokens[i]);
      }

      value.setPageRank(pageRank);
      value.setOutlinks(outlinks);
      value.setNodeId(tokens[0]);
    } else {
      key = null;
      value = null;
    }
    return gotNextKeyValue;
  }
}
