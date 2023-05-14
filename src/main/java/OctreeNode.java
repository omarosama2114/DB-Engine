import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Vector;

public class OctreeNode implements Serializable{
    Comparable minX , maxX ,minY , maxY ,minZ , maxZ;
    Vector<Tuple> data;
    TreeMap<Tuple, Vector<RecordReference>> records;
    OctreeNode[] children;
    boolean dummy;
    OctreeNode(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ){
        data = new Vector<>();
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.maxX = maxX;
        this.maxY = maxY;
        this.maxZ = maxZ;
        this.children = new OctreeNode[8];
        dummy = false;
        this.data = new Vector<>();
        records = new TreeMap<>();
    }

    void add(Comparable x, Comparable y, Comparable z, RecordReference record) {
        Tuple newTuple = new Tuple(x, y, z);
        Vector<RecordReference> myData = records.getOrDefault(newTuple, new Vector<>());
        if(myData.size() == 0)data.add(newTuple);
        myData.add(record);
        records.put(newTuple, myData);
    }

    void add(Comparable x, Comparable y, Comparable z, Vector<RecordReference   > records){
        Tuple newTuple = new Tuple(x, y, z);
        Vector<RecordReference> myData = this.records.getOrDefault(newTuple, new Vector<>());
        if(myData.size() == 0)data.add(newTuple);
        myData.addAll(records);
        this.records.put(newTuple, myData);
    }
}