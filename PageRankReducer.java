import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
        extends Reducer<Text, PageRankWrapper, Text, Node> {
    private IntWritable result = new IntWritable();
    private Text nodeName;

    public void reduce(Text key, Iterable<PageRankWrapper> values,
                       Context context
    ) throws IOException, InterruptedException {
        Node node = new Node();
        double sum = 0;
        for (PageRankWrapper v : values) {
            if (v.isNode()) {
                node = v.getNode();
            } else {
                sum += v.getPageMass();
            }
        }
        if (Math.abs(node.getPageRank() - sum) > context.getConfiguration().getDouble("convergence", 0)) {
            context.getCounter(CounterKeys.NEEDS_RUN).increment(1);
        }
//        System.out.println(node);
//        System.out.println("New Rank: " + sum);
        node.setPageRank(sum);
        context.write(key, node);
    }
}
