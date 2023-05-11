public class OctreeNode <X , Y , Z>{
    X x , minX , maxX;
    Y y , minY , maxY;
    Z z  , minZ , maxZ;
    OctreeNode<X , Y , Z>[] children;
    boolean hasChildren;
    OctreeNode(X x, Y y, Z z, X minX, Y minY, Z minZ, X maxX, Y maxY, Z maxZ) {
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