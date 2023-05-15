import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Octree implements Serializable{
    OctreeNode root;  // root node of the octree
    int nodeSize;
    Octree(Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ) throws IOException {
        this.root = new OctreeNode(minX, minY, minZ, maxX, maxY, maxZ);
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        FileInputStream fileInputStream = new FileInputStream(fileName);
        prop.load(fileInputStream);
        this.nodeSize = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
    }

    void reset(){
        for(int i = 0; i<8; i++){
            root.children[i] = null;
        }
        root.dummy = false;
        root.data.clear();
        root.records.clear();
    }

    void insert(Comparable x, Comparable y, Comparable z, RecordReference record) {
        if(this.root.data.size() == 0) {
            this.root.add(x, y, z, record);
            return;
        }
        OctreeNode node = this.root;
        while (node.dummy) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            if (node.children[index] == null) {
                node.children[index] = new OctreeNode(cur[1],cur[2], cur[3],cur[4], cur[5], cur[6]);
                return;
            }
            node = node.children[index];
        }
        if(node.data.size() < nodeSize){
            node.add(x, y, z, record);
            return;
        }
        do {

            node.dummy = true;
            Comparable[][] cur = new Comparable[nodeSize][];
            Comparable[] toBeInserted = getIndex(node, x, y, z);
            boolean extreme = true;
            for(int i=0; i<nodeSize; i++){
                cur[i] = getIndex(node, node.data.get(i).x, node.data.get(i).y, node.data.get(i).z);
                if((int)cur[i][0] != (int)toBeInserted[0])
                extreme = false;
            }
            
            if (!extreme) {
                node.children[(int)toBeInserted[0]] = new OctreeNode(toBeInserted[1], toBeInserted[2], toBeInserted[3],
                            toBeInserted[4], toBeInserted[5], toBeInserted[6]);
                node.children[(int)toBeInserted[0]].add(x, y, z, record);
                for(int i=0; i<nodeSize; i++){
                    Tuple current = node.data.get(i);
                    if(node.children[(int)cur[i][0]] == null){
                        node.children[(int)cur[i][0]] = new OctreeNode(cur[i][1],cur[i][2], cur[i][3],cur[i][4], cur[i][5], cur[i][6]);
                        node.children[(int)cur[i][0]].add(current.x, current.y, current.z, node.records.get(current));
                    }
                    else{
                        node.children[(int)cur[i][0]].add(current.x, current.y, current.z, node.records.get(current));
                    }
                }
                node.data.clear();
                node.records.clear();
                break;
            } else {
                node.children[(int)toBeInserted[0]] = new OctreeNode(toBeInserted[1], toBeInserted[2], toBeInserted[3],  
                            toBeInserted[4],  toBeInserted[5], toBeInserted[6]);
                node.children[(int)toBeInserted[0]].data = (Vector<Tuple>)node.data.clone();
                node.children[(int) toBeInserted[0]].records = (TreeMap<Tuple, Vector<RecordReference>>) node.records.clone();
                node.data.clear();
                node.records.clear();
                node = node.children[(int)toBeInserted[0]];
            }
        } while (true);
    }

    //x -> min to midPoint
    //y -> midPoint to max
    //yyy, yyx, yxy, yxx, xyy, xyx, xxy, xxx

    String getMiddleString(String S, String T) {
        int j = Math.min(S.length(), T.length());
        while (j < Math.max(S.length(), T.length())) {
            if (S.length() < T.length()) S += T.charAt(j++);
            else T += S.charAt(j++);
        }
        int N = S.length();
        int[] a1 = new int[N + 1];
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }
        for (int i = 0; i <= N; i++) {
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }
            a1[i] = (int) a1[i] / 2;
        }
        String s = "";
        for (int i = 1; i <= N; i++)
            s += ((char) (a1[i] + 97));
        return s.toString();
    }

    Comparable getMid(Comparable minX, Comparable maxX) {
        if (minX instanceof Integer)
            return ((int) minX + (int) maxX) / 2;
        else if (minX instanceof Double) {
            return ((double) minX + (double) maxX) / 2;
        } else if (minX instanceof String) {
            return getMiddleString((String) minX, (String) maxX);
        }
        return new Date((((Date) minX).getTime() + ((Date) maxX).getTime()) / 2);
    }

    boolean compareTo(Comparable a, Comparable b) {
        return a.compareTo(b) >= 0;
    }

    Comparable[] getIndex(OctreeNode node, Comparable x, Comparable y, Comparable z) {
        int index = -1;
        Comparable midX =  getMid(node.minX, node.maxX);
        Comparable midY =  getMid(node.minY, node.maxY);
        Comparable midZ =  getMid(node.minZ, node.maxZ);
        Comparable minX, maxX;
        Comparable minY, maxY;
        Comparable minZ, maxZ;
        if (compareTo( x,  midX)) {
            if (compareTo( y,  midY)) {
                if (compareTo( z, midZ)) {
                    index = 0;
                    minX = midX;
                    minY = midY;
                    minZ = midZ;
                    maxX = node.maxX;
                    maxY = node.maxY;
                    maxZ = node.maxZ;
                } else {
                    index = 1;
                    minX = midX;
                    minY = midY;
                    minZ = node.minZ;
                    maxX = node.maxX;
                    maxY = node.maxY;
                    maxZ = midZ;
                }
            } else {
                if (compareTo(z, midZ)) {
                    index = 2;
                    minX = midX;
                    minY = node.minY;
                    minZ = midZ;
                    maxX = node.maxX;
                    maxY = midY;
                    maxZ = node.maxZ;
                } else {
                    index = 3;
                    minX = midX;
                    minY = node.minY;
                    minZ = node.minZ;
                    maxX = node.maxX;
                    maxY = midY;
                    maxZ = midZ;
                }
            }
        } else {
            if (compareTo( y, midY)) {
                if (compareTo( z,  midZ)) {
                    index = 4;
                    minX = node.minX;
                    minY = midY;
                    minZ = midZ;
                    maxX = midX;
                    maxY = node.maxY;
                    maxZ = node.maxZ;
                } else {
                    index = 5;
                    minX = node.minX;
                    minY = midY;
                    minZ = node.minZ;
                    maxX = midX;
                    maxY = node.maxY;
                    maxZ = midZ;
                }
            } else {
                if (compareTo( z, midZ)) {
                    index = 6;
                    minX = node.minX;
                    minY = node.minY;
                    minZ = midZ;
                    maxX = midX;
                    maxY = midY;
                    maxZ = node.maxZ;
                } else {
                    index = 7;
                    minX = node.minX;
                    minY = node.minY;
                    minZ = node.minZ;
                    maxX = midX;
                    maxY = midY;
                    maxZ = midZ;
                }
            }
        }
        return new Comparable[]{index, minX, minY, minZ, maxX, maxY, maxZ};

    }

    OctreeNode searchForRemoval(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = this.root, par = null;
        while (node != null && node.dummy) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            par = node;
            node = node.children[index];
        }
        return par;
    }

    void remove(Comparable x, Comparable y, Comparable z, Comparable clusteringKey) {
        OctreeNode node = search(x, y, z);
        if (node == null) return;
        OctreeNode par = searchForRemoval(x, y, z);
        int idx = -1;
        Vector<RecordReference> r = node.records.getOrDefault(new Tuple(x, y, z) , null);
        if(r == null) return;
        for(int i = 0; i < r.size(); i++) {
            if (r.get(i).clusteringKey.equals(clusteringKey)) {
                idx = i;
                break;
            }
        }

        if(idx == -1) return;
        r.remove(idx);

        if(r.size() == 0) {
            idx = -1;
            for(int i = 0; i < node.data.size(); i++) {
                if (node.data.get(i).x.equals(x) && node.data.get(i).y.equals(y) && node.data.get(i).z.equals(z)) {
                    idx = i;
                    break;
                }
            }
            if(idx != -1) {
                node.data.remove(idx);
                node.records.remove(new Tuple(x, y, z));
            }
        }

        if(par == null || node.data.size() != 0){
            return;
        }
        Comparable[] cur = getIndex(par, x, y, z);
        par.children[(int) cur[0]] = null;
    }

    OctreeNode search(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = this.root;
        while(node != null && node.dummy) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            node = node.children[index];
        }
        if(node == null) return null;

        for(int i = 0; i<node.data.size(); i++){
            Tuple current = node.data.get(i);
            if(current.x.compareTo(x) == 0 && current.y.compareTo(y) == 0 && current.z.compareTo(z) == 0){
                return node;
            }
        }
        return null;
    }

    Vector<RecordReference> searchReferences(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = this.root;
        Vector<RecordReference> references = new Vector<>();
        while(node != null && node.dummy) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            node = node.children[index];
        }
        if(node == null) return null;

        references = node.records.getOrDefault(new Tuple(x, y, z), new Vector<>());
        return references;
    }


    void update(Comparable oldX, Comparable oldY, Comparable oldZ, Comparable newX, Comparable newY, Comparable newZ, RecordReference record) {
        if(search(oldX, oldY, oldZ) == null) return;
        remove(oldX, oldY, oldZ, record.clusteringKey);
        insert(newX, newY, newZ, record);
    }

    void rename(HashMap<String , String> map, OctreeNode node) {
        if(node == null) return;
        if(node.dummy){
            for(int i = 0; i<node.children.length; i++){
                rename(map, node.children[i]);
            }
        }else{
            for(Tuple current : node.data){
                Vector<RecordReference> v = node.records.get(current);
                for(RecordReference r : v) {
                    String oldName = r.pageName;
                    String newName = map.getOrDefault(oldName , oldName);
                    r.pageName = newName;
                }
            }
        }
    }
}


