import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

public class Node implements Writable {

  private List<String> outlinks = new ArrayList<>();

  private double pageRank = 0.0;
  private String nodeId;

  public Node(){}

  public Node(String nodeId, double pageRank, List<String> outlinks){
    this.pageRank = pageRank;
    this.outlinks = outlinks;
    this.nodeId = nodeId;
  }

  public double getPageRank(){
    return this.pageRank;
  }

  public void setPageRank(double pageRank){
    this.pageRank = pageRank;
  }

  public void setOutlinks(List<String> outlinks){
    this.outlinks = outlinks;
  }

  public List<String> getOutlinks(){
    return this.outlinks;
  }

  public void setNodeId(String nodeId){
    this.nodeId = nodeId;
  }

  public String getNodeId(){
    return nodeId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeChars(nodeId);
    out.writeChars("\n");
    out.writeDouble(pageRank);
    out.writeInt(outlinks.size());
    for(String link : outlinks){
        out.writeChars(link);
        out.writeChars("\n");
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    nodeId = in.readLine();
    pageRank = in.readDouble();
    outlinks = new ArrayList<>();
    int numOutlinks = in.readInt();
    for (int i = 0; i < numOutlinks; i++){
      outlinks.add(in.readLine());
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
      return this.nodeId.hashCode();
  }
  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
      if (this == obj) {
          return true;
      }

      return this.nodeId.equals(((Node) obj).getNodeId());
  }

  @Override
  public String toString() {
      return nodeId + ": " + pageRank;
  }
}
