public class Octree {
    OctreeNode root;  // root node of the octree

    Octree(float x, float y, float z, float minX, float minY, float minZ, float maxX, float maxY, float maxZ) {
        this.root = new OctreeNode(x, y, z, minX, minY, minZ, maxX, maxY, maxZ);
    }

    //TODO: read Config file to get the max entries in the node
    void insert(float x, float y, float z) {
        OctreeNode node = this.root;
        while (node.hasChildren) {
            float[] cur = getIndex(node, x, y, z);
            int index = (int)cur[0];
            if(node.children[index] == null){
                node.children[index] = new OctreeNode(x, y, z, cur[1], cur[2], cur[3], cur[4], cur[5], cur[6]);
                return;
            }
            node = node.children[index];
        }
        do{
            node.hasChildren = true;
            float[] cur1 = getIndex(node, x, y, z);
            float[] cur2 = getIndex(node, node.x, node.y, node.z);
            int index1 = (int)cur1[0];
            int index2 = (int)cur2[0];
            if(index1 != index2) {
                node.children[index1] = new OctreeNode(x, y, z, cur1[1], cur1[2], cur1[3], cur1[4], cur1[5], cur1[6]);
                node.children[index2] = new OctreeNode(node.x, node.y, node.z, cur2[1], cur2[2], cur2[3], cur2[4], cur2[5], cur2[6]);
                break;
            }
            else{
                node.children[index1] = new OctreeNode(node.x, node.y, node.z, cur2[1], cur2[2], cur2[3], cur2[4], cur2[5], cur2[6]);
            }
        }while(true);
    }

    //x -> min to midPoint
    //y -> midPoint to max
    //yyy, yyx, yxy, yxx, xyy, xyx, xxy, xxx

    float[] getIndex(OctreeNode node, float x, float y, float z) {
        int index = -1;
        float midX = (node.minX + node.maxX) / 2;
        float midY = (node.minY + node.maxY) / 2;
        float midZ = (node.minZ + node.maxZ) / 2;
        float minX, minY, minZ, maxX, maxY, maxZ;
        if (x >= midX) {
            if (y >= midY) {
                if (z >= midZ) {
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
                if (z >= midZ) {
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
            if (y >= midY) {
                if (z >= midZ) {
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
                if (z >= midZ) {
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
        return new float[]{index, minX, minY, minZ, maxX, maxY, maxZ};

    }

    void remove(float x, float y, float z) {
        // TODO: implement the removal algorithm
        // TODO: add types (String, int, etc.), create new object to handle each type.

    }

    OctreeNode search(float x, float y, float z) {
        OctreeNode node = this.root;
        while (node != null && node.hasChildren) {
            float[] cur = getIndex(node, x, y, z);
            int index = (int)cur[0];
            node = node.children[index];
        }
        return node;
    }
}


