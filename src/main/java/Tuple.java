import java.io.Serializable;

public class Tuple implements Serializable{
    Comparable x;
    Comparable y;
    Comparable z;
    SerializablePageRecord record;
    Tuple(Comparable x, Comparable y, Comparable z, SerializablePageRecord record){
        this.x = x;
        this.y = y;
        this.z = z;
        this.record = record;
    }
}