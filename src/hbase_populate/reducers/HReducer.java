package hbase_populate.reducers;

import conts.DatasetConts;
import conts.HBaseConts;
import conts.TableConts;
import hbase_populate.model.MovKey;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 4/16/15.
 */
public class HReducer extends Reducer<MovKey, IntWritable, ImmutableBytesWritable, Writable> {

    private StringBuilder builder = new StringBuilder();
    private HTable mTableMovie;
    private HTable mTableRating;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        mTableMovie = getTableConnection(TableConts.TABLE_NAME_USR_MOV);
        mTableRating = getTableConnection(TableConts.TABLE_NAME_USR_RATING);
    }


    /**
     * get connection with table specified by table name
     * @param name String table name
     * @return HTable Connection
     * @throws IOException
     */
    private HTable getTableConnection(String name) throws IOException {
        HTable table = new HTable(HBaseConfiguration.create(), name);
        table.setAutoFlush(true);
        table.setWriteBufferSize(HBaseConts.MB_100);

        return table;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mTableMovie.close();
        mTableRating.close();
    }

    @Override
    protected void reduce(MovKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        long count = 0;
        for(IntWritable v : values){
            builder.append(key.movie_id);
            builder.append(DatasetConts.SEPARATOR);
            count++;
            sum = sum + v.get();
        }


        addRow(mTableMovie, key.user, TableConts.FAMILY_USR_MOV, TableConts.TABLE_USR_MOV_COLUMN_LIST_MOV, builder.toString());
        addRow(mTableRating, key.user, TableConts.FAMILY_USR_RATING, TableConts.TABLE_COL_RATING, String.valueOf(sum / count));

        builder.setLength(0);

    }

    /**
     *  add row to the table
     * @param table HTable table connection
     * @param key String row key
     * @param family String table family
     * @param column String column name
     * @param value String column value
     * @throws IOException
     */
    private void addRow(HTable table, String key, String family, String column, String value) throws IOException {
        Put row = new Put(Bytes.toBytes(key));
        row.add(family.getBytes(),
                column.getBytes(), value.getBytes());
        table.put(row);
    }
}
