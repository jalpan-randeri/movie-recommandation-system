package kmeans.model;

/**
 * Created by jalpanranderi on 4/20/15.
 */
public class RatYear {
    public int avg_rating;
    public int avg_year;


    public RatYear(int year, int rating) {
        avg_year = year;
        avg_rating = rating;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatYear)) return false;

        RatYear ratYear = (RatYear) o;

        if (avg_rating != ratYear.avg_rating) return false;
        return avg_year == ratYear.avg_year;

    }

    @Override
    public int hashCode() {
        int result = avg_rating;
        result = 31 * result + avg_year;
        return result;
    }
}
