package kmeans.model;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jalpanranderi on 4/20/15.
 */
public class EmitValue implements WritableComparable<EmitValue> {

    public long user_id;
    public DoubleWritable rating;
    public DoubleWritable year;


    public EmitValue() {
        rating = new DoubleWritable();
        year = new DoubleWritable();
    }

    public EmitValue(long user_id, double rating, double year) {
        this.user_id = user_id;
        this.rating = new DoubleWritable(rating);
        this.year = new DoubleWritable(year);
    }


    @Override
    public int compareTo(EmitValue o) {
        if(user_id == o.user_id){
            if(rating == o.rating){
                return year.compareTo(o.year);
            }
            return  rating.compareTo(o.rating);
        }
        return Long.compare(user_id, o.user_id);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, user_id);
        rating.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        user_id = WritableUtils.readVLong(dataInput);
        rating.readFields(dataInput);
        year.readFields(dataInput);
    }
}
