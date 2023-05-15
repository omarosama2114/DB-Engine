import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

public class SelectIterator implements Iterator{
    Vector<SerializablePageRecord> data;
    int cur;
    public SelectIterator(Vector<SerializablePageRecord> arr){
        data = arr;
        cur = 0;
    }
    @Override
    public boolean hasNext() {
        return cur < data.size();
    }
    //TODO: return the recordHash or SerializablePageRecord itSelf?
    @Override
    public Object next() {
        return data.get(cur++).recordHash;
    }
}
