import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

public class OctreeNode implements Serializable{
    Comparable minX , maxX ,minY , maxY ,minZ , maxZ;
    Vector<Tuple> data;
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
    }

    void add(Comparable x, Comparable y, Comparable z, SerializablePageRecord record) {
        data.add(new Tuple(x, y, z, record));
    }
}