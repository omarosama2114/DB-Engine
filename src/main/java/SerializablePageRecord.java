
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class SerializablePageRecord implements Serializable {

    Hashtable<String, Object> recordHash;
    Comparable clusteringKey;

    public SerializablePageRecord(Hashtable<String, Object> recordHash, Comparable clusteringKey) {
        this.recordHash = (Hashtable<String, Object>) recordHash.clone();
        this.clusteringKey = clusteringKey;
    }
}
