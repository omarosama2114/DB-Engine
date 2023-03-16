import java.io.Serializable;

public class SerializablePageRecord implements Serializable {

    String afterSerialization;
    public SerializablePageRecord(String values){
        this.afterSerialization = values;


    }

    public String toString(){
        return afterSerialization;
    }
}
