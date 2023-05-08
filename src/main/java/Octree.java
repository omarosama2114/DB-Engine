public class Octree {
    OctreeNode root;  // root node of the octree

    Octree(float x, float y, float z, float minX, float minY, float minZ, float maxX, float maxY, float maxZ) {
        this.root = new OctreeNode(x, y, z, minX, minY, minZ, maxX, maxY, maxZ);
    }

    void insert(float x, float y, float z) {
        OctreeNode node = this.root;
        while (node.hasChildren) {
            int index = getIndex(node, x, y, z);

            node = node.children[index];
        }
       // node.children[getIndex(node, x, y, z)] = new OctreeNode(x, y, z, node.size / 2);
        node.hasChildren = true;
    }

    //x -> min to midPoint
    //y -> midPoint to max
    //yyy, yyx, yxy, yxx, xyy, xyx, xxy, xxx

    int getIndex(OctreeNode node, float x, float y, float z) {
        int index = -1;
        float midX = (node.minX + node.maxX) / 2;
        float midY = (node.minY + node.maxY) / 2;
        float midZ = (node.minZ + node.maxZ) / 2;
        if (x >= midX) {
            if (y >= midY) {
                if (z >= midZ) {
                    index = 0;
                } else {
                    index = 1;
                }
            } else {
                if (z >= midZ) {
                    index = 2;
                } else {
                    index = 3;
                }
            }
        } else {
            if (y >= midY) {
                if (z >= midZ) {
                    index = 4;
                } else {
                    index = 5;
                }
            } else {
                if (z >= midZ) {
                    index = 6;
                } else {
                    index = 7;
                }
            }
        }
        return index;

    }

    void remove(float x, float y, float z) {
        // TODO: implement the removal algorithm
        // TODO: add types (String, int, etc.)

    }

    OctreeNode search(float x, float y, float z) {
        OctreeNode node = this.root;
        while (node != null && node.hasChildren) {
            int index = getIndex(node, x, y, z);
            node = node.children[index];
        }
        return node;
    }
}


