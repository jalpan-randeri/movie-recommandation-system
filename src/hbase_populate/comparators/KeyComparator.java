package hbase_populate.comparators;

import hbase_populate.model.MovKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by jalpanranderi on 4/16/15.
 */
public class KeyComparator extends WritableComparator {

    protected KeyComparator() {
        super(MovKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovKey key1 = (MovKey) a;
        MovKey key2 = (MovKey) b;

        return key1.compareTo(key2);
    }
}
