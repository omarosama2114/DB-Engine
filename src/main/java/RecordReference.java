import java.io.Serializable;

public class RecordReference implements Serializable {
    String pageName;
    Comparable clusteringKey;
    RecordReference(String pageName, Comparable clusteringKey) {
        this.pageName = pageName;
        this.clusteringKey = clusteringKey;
    }
}
