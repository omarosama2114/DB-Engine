import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Test {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream("Student/1.class");
        ObjectInputStream ois = new ObjectInputStream(fis);
        Page b = (Page) ois.readObject();
        for(SerializablePageRecord s : b.pageData){
            for(String key : s.recordHash.keySet()){
                System.out.println(key + " " + s.recordHash.get(key));
            }
            System.out.println("=========");
        }

        ois.close();
        fis.close();
    }

}
