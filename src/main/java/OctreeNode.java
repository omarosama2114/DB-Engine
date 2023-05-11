import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

public class OctreeNode{
    Comparable minX , maxX ,minY , maxY ,minZ , maxZ;
    Vector<tuple> data;
    OctreeNode[] children;
    boolean hasChildren;
    OctreeNode(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ){
        data = new Vector<>();
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.maxX = maxX;
        this.maxY = maxY;
        this.maxZ = maxZ;
        this.children = new OctreeNode[8];
        this.hasChildren = false;
        this.data = new Vector<>();
    }

    void add(Comparable x, Comparable y, Comparable z) {
        data.add(new tuple(x, y, z));
    }
    static class tuple{
        Comparable x;
        Comparable y;
        Comparable z;

        SerializablePageRecord record;
        tuple(Comparable x, Comparable y, Comparable z){
            this.x = x;
            this.y = y;
            this.z = z;
        }
    }
}