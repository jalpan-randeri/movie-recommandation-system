package kmeans.model;

/**
 * Created by jalpanranderi on 4/20/15.
 */
public class Centroid {
    public double rating_x;
    public double year_y;

    public Centroid(double rating_x, double year_y) {
        this.rating_x = rating_x;
        this.year_y = year_y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Centroid)) return false;

        Centroid centroid = (Centroid) o;

        if (rating_x != centroid.rating_x) return false;
        return year_y == centroid.year_y;

    }

    @Override
    public int hashCode() {
        double result =(int) rating_x;
        result = 31 * result + year_y;
        return (int) result;
    }

    @Override
    public String toString() {
        return "Centroid{" +
                "rating_x=" + rating_x +
                ", year_y=" + year_y +'}';
    }
}
