import java.util.Date;

public class OctreeTest {
    public static void main(String[] args) {
        try {
            // Create an instance of Octree
            Octree octree = new Octree(null, 0, 0, 0, -100, -100, -100, 100, 100, 100);



            //TODO handle duplicates
            //TODO finish testing



            // Insert data points
//            octree.insert(1, 2, 3, null);
//            octree.insert(4, 5, 6, null);
//            octree.insert(-1, -2, -3, null);
//            octree.insert(10, 20, 30, null);
//
//            // Search for a data point
//            OctreeNode node = octree.search(4, 5, 6);
//            if (node != null) {
//                System.out.println("Data point found in the Octree!");
//            } else {
//                System.out.println("Data point not found in the Octree.");
//            }
//
//            // Remove a data point
//            octree.remove(4, 5, 6);
//            node = octree.search(4, 5, 6);
//            if (node != null) {
//                System.out.println("Data point found in the Octree after removal!");
//            } else {
//                System.out.println("Data point not found in the Octree after removal.");
//            }
//
//            // Update a data point
//            octree.update(1, 2, 3, 100, 200, 300, null);
//            node = octree.search(1, 2, 3);
//            if (node == null) {
//                System.out.println("Data point updated successfully!");
//            } else {
//                System.out.println("Data point update failed.");
//            }
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            octree.insert(1, 2, 3, null);
            OctreeNode node = octree.search(1, 2, 3);
            System.out.println("Number of occurrences: " + node.data.size());



//            octree.insert(-10, -20, -30, null);
//            octree.insert(-5, -5, -5, null);
//            OctreeNode node1 = octree.search(-10, -20, -30);
//            OctreeNode node2 = octree.search(-5, -5, -5);
//            System.out.println("Data point 1 found: " + (node1 != null));
//            System.out.println("Data point 2 found: " + (node2 != null));



//            octree.insert(1, 2, 3, null);
//            octree.remove(4, 5, 6);
//            OctreeNode node = octree.search(4, 5, 6);
//            System.out.println("Data point found: " + (node != null));


//            octree.insert(1, 2, 3, null);
//            octree.update(4, 5, 6, 10, 11, 12, null);
//            OctreeNode node = octree.search(10, 11, 12);
//            System.out.println("Data point found: " + (node != null));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}