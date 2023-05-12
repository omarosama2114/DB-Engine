import java.io.Serializable;

public class Tuple implements Serializable, Comparable{
    Comparable x;
    Comparable y;
    Comparable z;
    Tuple(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    @Override
    public int compareTo(Object o) {
        Tuple current = (Tuple) o;
        if(current.x.equals(this.x)){
            if(current.y.equals(this.y)) return current.z.compareTo(this.z);
            return current.y.compareTo(this.y);
        }
        return current.x.compareTo(this.x);
    }
}