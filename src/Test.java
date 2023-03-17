// Java program to illustrate Serializable interface
import java.io.*;

// By implementing Serializable interface
// we make sure that state of instances of class A
// can be saved in a file.
public class Test {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException
    {
        //SerializablePageRecord a = new SerializablePageRecord("GeeksForGeeks");

        // Serializing 'a'
        FileOutputStream fos
                = new FileOutputStream("xyz.class");
        ObjectOutputStream oos
                = new ObjectOutputStream(fos);
        oos.writeObject(a);

        // De-serializing 'a'
        FileInputStream fis
                = new FileInputStream("xyz.class");
        ObjectInputStream ois = new ObjectInputStream(fis);
        SerializablePageRecord b = (SerializablePageRecord) ois.readObject(); // down-casting object
        System.out.println(b.afterSerialization);
        // closing streams
        oos.close();
        ois.close();
    }
}
