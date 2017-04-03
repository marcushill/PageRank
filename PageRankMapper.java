import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Node, Text, PageRankWrapper>{

  public void map(Text key, Node value, Context context
                  ) throws IOException, InterruptedException {
    String keyString = key.toString().trim().replace("\0", "");
    context.write(new Text(keyString), new PageRankWrapper(value));
    double pageMass = value.getPageRank() / (double)value.getOutlinks().size();

    for(String node : value.getOutlinks()){
      context.write(new Text(node.trim().replace("\0", "")),
                    new PageRankWrapper(pageMass));
    }

  }
}
