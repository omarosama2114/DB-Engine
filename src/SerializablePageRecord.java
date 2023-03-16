import java.io.Serializable;

public class SerializablePageRecord implements Serializable, Comparable {

    String afterSerialization;
    int clusteringKey;
    public SerializablePageRecord(String values){
        this.afterSerialization = values;
        String[] splitted = afterSerialization.split(",");
        this.clusteringKey = Integer.parseInt(splitted[0]);
    }

    public String toString(){
        return afterSerialization;
    }

    @Override
    public int compareTo(Object o) {
        return this.clusteringKey - ((SerializablePageRecord) o).clusteringKey;
    }
}
