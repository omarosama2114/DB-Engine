import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class Octree <X,Y,Z> {
    OctreeNode<X, Y, Z> root;  // root node of the octree

    Octree(X x, Y y, Z z, X minX, Y minY, Z minZ, X maxX, Y maxY, Z maxZ) {
        this.root = new OctreeNode<>(x, y, z, minX, minY, minZ, maxX, maxY, maxZ);
    }

    //TODO: read Config file to get the max entries in the node
    void insert(X x, Y y, Z z) {
        OctreeNode<X, Y, Z> node = this.root;
        while (node.hasChildren) {
            Object[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            if (node.children[index] == null) {
                node.children[index] = new OctreeNode<X, Y, Z>(x, y, z, (X) cur[1], (Y) cur[2], (Z) cur[3], (X) cur[4], (Y) cur[5], (Z) cur[6]);
                return;
            }
            node = node.children[index];
        }
        do {
            node.hasChildren = true;
            Object[] cur1 = getIndex(node, x, y, z);
            Object[] cur2 = getIndex(node, node.x, node.y, node.z);
            int index1 = (int) cur1[0];
            int index2 = (int) cur2[0];
            if (index1 != index2) {
                node.children[index1] = new OctreeNode<>(x, y, z, (X) cur1[1], (Y) cur1[2], (Z) cur1[3], (X) cur1[4], (Y) cur1[5], (Z) cur1[6]);
                node.children[index2] = new OctreeNode<>(node.x, node.y, node.z, (X) cur2[1], (Y) cur2[2], (Z) cur2[3], (X) cur2[4], (Y) cur2[5], (Z) cur2[6]);
                break;
            } else {
                node.children[index1] = new OctreeNode<>(x, y, z, (X) cur1[1], (Y) cur1[2], (Z) cur1[3], (X) cur1[4], (Y) cur1[5], (Z) cur1[6]);
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

    Object getMid(Object minX, Object maxX) {
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

    Object[] getIndex(OctreeNode<X, Y, Z> node, X x, Y y, Z z) {
        int index = -1;
        X midX = (X) getMid(node.minX, node.maxX);
        Y midY = (Y) getMid(node.minY, node.maxY);
        Z midZ = (Z) getMid(node.minZ, node.maxZ);
        X minX, maxX;
        Y minY, maxY;
        Z minZ, maxZ;
        if (compareTo((Comparable) x, (Comparable) midX)) {
            if (compareTo((Comparable) y, (Comparable) midY)) {
                if (compareTo((Comparable) z, (Comparable) midZ)) {
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
                if (compareTo((Comparable) z, (Comparable) midZ)) {
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
            if (compareTo((Comparable) y, (Comparable) midY)) {
                if (compareTo((Comparable) z, (Comparable) midZ)) {
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
                if (compareTo((Comparable) z, (Comparable) midZ)) {
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
        return new Object[]{index, minX, minY, minZ, maxX, maxY, maxZ};

    }

    OctreeNode<X, Y, Z> searchForRemoval(X x, Y y, Z z) {
        OctreeNode<X, Y, Z> node = this.root, par = null;
        while (node != null && node.hasChildren) {
            Object[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            par = node;
            node = node.children[index];
        }
        return par;
    }

    void remove(X x, Y y, Z z) {
        OctreeNode<X, Y, Z> node = search(x, y, z);
        if (node == null) return;
        OctreeNode<X, Y, Z> par = searchForRemoval(x, y, z);
        Object[] cur = getIndex(par, x, y, z);
        par.children[(int) cur[0]] = null;
    }

    OctreeNode<X, Y, Z> search(X x, Y y, Z z) {
        OctreeNode<X, Y, Z> node = this.root;
        while (node != null && node.hasChildren) {
            Object[] cur = getIndex(node, x, y, z);
            int index = (int) cur[0];
            node = node.children[index];
        }
        return node;
    }

    void update(X oldX, Y oldY, Z oldZ, X newX, Y newY, Z newZ) {
        remove(oldX, oldY, oldZ);
        insert(newX, newY, newZ);
    }
}


