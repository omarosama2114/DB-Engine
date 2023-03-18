import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class SerializablePageRecord implements Serializable, Comparable {

    String afterSerialization;
    int clusteringKey;

    Vector<String> keys;
    public SerializablePageRecord(String values, int clusteringKey) {
        this.afterSerialization = values;
        this.clusteringKey = clusteringKey;
    }

    public String toString() {
        return afterSerialization;
    }

    @Override
    public int compareTo(Object o) {
        return this.clusteringKey - ((SerializablePageRecord) o).clusteringKey;
    }
}
