package hbase_populate.model;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jalpanranderi on 4/16/15.
 */
public class MovKey implements WritableComparable<MovKey> {

    public String user;
    public String movie_id;



    public MovKey() {
        user = "";
        movie_id = "";
    }

    public MovKey(String user, String movie_id) {
        this.user = user;
        this.movie_id = movie_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MovKey)) return false;

        MovKey movKey = (MovKey) o;

        if (!user.equals(movKey.user)) return false;
        return movie_id.equals(movKey.movie_id);

    }

    @Override
    public int hashCode() {
        int result = user.hashCode();
        result = 31 * result + movie_id.hashCode();
        return result;
    }

    @Override
    public int compareTo(MovKey o) {
        long this_usr = Long.parseLong(user);
        long that_user = Long.parseLong(o.user);
        int ans = Long.compare(this_usr, that_user);

        if(ans == 0) {
            long this_mov = Long.parseLong(movie_id);
            long that_mov = Long.parseLong(o.movie_id);

            return Long.compare(this_mov, that_mov);
        }

        return ans;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, user);
        WritableUtils.writeString(dataOutput, movie_id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        user = WritableUtils.readString(dataInput);
        movie_id = WritableUtils.readString(dataInput);
    }
}
