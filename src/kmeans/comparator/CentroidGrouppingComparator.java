package kmeans.comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by jalpanranderi on 4/12/15.
 */
public class CentroidGrouppingComparator extends WritableComparator {

    protected CentroidGrouppingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text i1 = (Text) a;
        Text i2 = (Text) b;

        int c1 = Integer.parseInt(a.toString().split("\\$")[1]);
        int c2 = Integer.parseInt(b.toString().split("\\$")[1]);

        return c1 - c2;
    }
}
