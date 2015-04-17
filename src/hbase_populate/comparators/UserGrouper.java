package hbase_populate.comparators;

import hbase_populate.model.MovKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by jalpanranderi on 4/16/15.
 */
public class UserGrouper extends WritableComparator {

    protected UserGrouper() {
        super(MovKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovKey key1 = (MovKey) a;
        MovKey key2 = (MovKey) b;

        long u1 = Long.parseLong(key1.user);
        long u2 = Long.parseLong(key2.user);

        return Long.compare(u1, u2);
    }

}