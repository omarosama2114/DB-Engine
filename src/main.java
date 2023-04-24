import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Hashtable;

public class main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        Hashtable htblColNameValue = new Hashtable( );
        // Hashtable htblColNameType = new Hashtable( );
        // htblColNameType.put("id", "java.lang.Integer");
        // htblColNameType.put("name", "java.lang.String");
        // htblColNameType.put("gpa", "java.lang.Double");
        // Hashtable htblColNameMin = new Hashtable();
        // htblColNameMin.put("id", "0");
        // htblColNameMin.put("name", "A");
        // htblColNameMin.put("gpa", "0");
        // Hashtable htblColNameMax = new Hashtable();
        // htblColNameMax.put("id", "100000000");
        // htblColNameMax.put("name", "ZZZZZZZZZZZZZZZZZ");
        // htblColNameMax.put("gpa", "10000000");
        // dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
        // htblColNameValue.put("id", new Integer( 2343432 ));
        // htblColNameValue.put("name", new String("Ahmed Noor" ) );
        // htblColNameValue.put("gpa", new Double( 0.95 ) );
        // dbApp.insertIntoTable( strTableName , htblColNameValue );
        // htblColNameValue.clear( );
        // htblColNameValue.put("id", new Integer( 453455 ));
        // htblColNameValue.put("name", new String("Ahmed Noor" ) );
        // htblColNameValue.put("gpa", new Double( 0.95 ) );
        // dbApp.insertIntoTable( strTableName , htblColNameValue );
        // htblColNameValue.clear( );
        // htblColNameValue.put("id", new Integer( 5674567 ));
        // htblColNameValue.put("name", new String("Dalia Noor" ) );
        // htblColNameValue.put("gpa", new Double( 1.25 ) );
        // dbApp.insertIntoTable( strTableName , htblColNameValue );
        // htblColNameValue.clear( );
        // htblColNameValue.put("id", new Integer( 23498 ));
        // htblColNameValue.put("name", new String("John Noor" ) );
        // htblColNameValue.put("gpa", new Double( 1.5 ) );
        // dbApp.insertIntoTable( strTableName , htblColNameValue );
        // htblColNameValue.clear( );
        // htblColNameValue.put("id", new Integer( 78452 ));
        // htblColNameValue.put("name", new String("ZAKY NOOR" ) );
        // htblColNameValue.put("gpa", new Double( 0.88 ) );
        // dbApp.insertIntoTable( strTableName , htblColNameValue );
        // htblColNameValue.clear();
       htblColNameValue.put("id", new Integer(78452));

    //    htblColNameValue.put("gpa", new Double( 1.23));
    //    dbApp.updateTable(strTableName, "78452", htblColNameValue);
       dbApp.deleteFromTable(strTableName, htblColNameValue);
       htblColNameValue.clear();
       htblColNameValue.put("id", new Integer(2343432));
       dbApp.deleteFromTable(strTableName, htblColNameValue);

    }
}
