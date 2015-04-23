package kmeans.model;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jalpanranderi on 4/20/15.
 */
public class EmitValue implements WritableComparable<EmitValue> {

    public String user_id;
    public DoubleWritable rating;
    public DoubleWritable year;


    public EmitValue() {
        user_id = "NULL";
        rating = new DoubleWritable();
        year = new DoubleWritable();
    }

    public EmitValue(String user_id, double rating, double year) {
        this.user_id = user_id;
        this.rating = new DoubleWritable(rating);
        this.year = new DoubleWritable(year);
    }


    @Override
    public boolean equals(Object o) {
        EmitValue other = (EmitValue) o;
        return user_id.equals(other.user_id);
    }

    @Override
    public int hashCode() {
        return user_id.hashCode();
    }

    @Override
    public int compareTo(EmitValue o) {
        return user_id.compareTo(o.user_id);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, user_id);
        rating.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        user_id = WritableUtils.readString(dataInput);
        rating.readFields(dataInput);
        year.readFields(dataInput);
    }
}
