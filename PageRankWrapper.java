import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageRankWrapper implements Writable {

    private Node node;
    private double pageMass;

    public PageRankWrapper() {
    }

    public PageRankWrapper(Node node) {
        this.node = node;
    }

    public PageRankWrapper(double pageMass) {
        this.pageMass = pageMass;
    }

    public boolean isNode() {
        return node != null;
    }

    /**
     * Returns value of node
     *
     * @return
     */
    public Node getNode() {
        return node;
    }

    /**
     * Sets new value of node
     *
     * @param
     */
    public void setNode(Node node) {
        this.node = node;
    }

    /**
     * Returns value of pageMass
     *
     * @return
     */
    public double getPageMass() {
        return pageMass;
    }

    /**
     * Sets new value of pageMass
     *
     * @param
     */
    public void setPageMass(double pageMass) {
        this.pageMass = pageMass;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.isNode());
        if (this.isNode()) {
            node.write(out);
        } else {
            out.writeDouble(pageMass);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        boolean isNode = in.readBoolean();
        if (isNode) {
            this.node = new Node();
            node.readFields(in);
        } else {
            this.node = null;
            pageMass = in.readDouble();
        }
    }
}
