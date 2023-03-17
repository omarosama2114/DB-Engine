import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {

    Vector<SerializablePageRecord> pageData;

    public void addRecord(SerializablePageRecord data) {
        pageData.add(data);
    }

}
