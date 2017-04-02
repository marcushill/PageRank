import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;

public class PageRankInputFormat extends FileInputFormat<Text, Node> {

  @Override
  public RecordReader<Text,Node> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {

    return new PageRankRecordReader();
  }
}
