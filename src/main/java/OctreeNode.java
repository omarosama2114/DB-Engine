public class OctreeNode {
    float x, y, z;
    float minX;
    float minY;
    float minZ;
    float maxX;
    float maxY;
    float maxZ;
    OctreeNode[] children;
    boolean hasChildren;

    OctreeNode(float x, float y, float z, float minX, float minY, float minZ, float maxX, float maxY, float maxZ) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.maxX = maxX;
        this.maxY = maxY;
        this.maxZ = maxZ;
        this.children = new OctreeNode[8];
        this.hasChildren = false;
    }
}

