import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

public class Octree {
    OctreeNode root;  // root node of the octree
    int nodeSize;

    Octree(Comparable x, Comparable y, Comparable z, Comparable minX, Comparable minY, Comparable minZ, Comparable maxX, Comparable maxY, Comparable maxZ) throws IOException {
        this.root = new OctreeNode(minX, minY, minZ, maxX, maxY, maxZ);
        root.data.add(new OctreeNode.tuple(x , y , z));
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        FileInputStream fileInputStream = new FileInputStream(fileName);
        prop.load(fileInputStream);
        this.nodeSize = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
    }

    //TODO: read Config file to get the max entries in the node
    void insert(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = this.root;
        while (node.hasChildren) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            if (node.children[index] == null) {
                node.children[index] = new OctreeNode(cur[1],cur[2], cur[3],cur[4], cur[5], cur[6]);
                return;
            }
            node = node.children[index];
        }
        do {
            node.hasChildren = true;
            Comparable[] cur1 = getIndex(node, x, y, z);
            Comparable[] cur2 = getIndex(node, node.x, node.y, node.z);
            int index1 = (int) cur1[0];
            int index2 = (int) cur2[0];
            if (index1 != index2) {
                node.children[index1] = new OctreeNode(x, y, z, cur1[1], cur1[2], cur1[3], cur1[4], cur1[5],cur1[6]);
                node.children[index2] = new OctreeNode(node.x, node.y, node.z, cur2[1], cur2[2], cur2[3], cur2[4],cur2[5], cur2[6]);
                break;
            } else {
                node.children[index1] = new OctreeNode(x, y, z, cur1[1], cur1[2], cur1[3],  cur1[4],  cur1[5], cur1[6]);
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
        while (node != null && node.hasChildren) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            par = node;
            node = node.children[index];
        }
        return par;
    }

    void remove(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = search(x, y, z);
        if (node == null) return;
        OctreeNode par = searchForRemoval(x, y, z);
        Comparable[] cur = getIndex(par, x, y, z);
        par.children[(int) cur[0]] = null;
    }

    OctreeNode search(Comparable x, Comparable y, Comparable z) {
        OctreeNode node = this.root;
        while (node != null && node.hasChildren) {
            Comparable[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            node = node.children[index];
        }
        return node;
    }

    void update(Comparable oldX, Comparable oldY, Comparable oldZ, Comparable newX, Comparable newY, Comparable newZ) {
        remove(oldX, oldY, oldZ);
        insert(newX, newY, newZ);
    }
}


