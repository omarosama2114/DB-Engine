
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

public class main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException, CsvException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();
        Hashtable htblColNameValue = new Hashtable();
//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        Hashtable htblColNameMin = new Hashtable();
//        htblColNameMin.put("id", "0");
//        htblColNameMin.put("name", "A");
//        htblColNameMin.put("gpa", "0");
//        Hashtable htblColNameMax = new Hashtable();
//        htblColNameMax.put("id", "10000000");
//        htblColNameMax.put("name", "ZZZZZZZZZZZZZZZZZ");
//        htblColNameMax.put("gpa", "10000000");
//        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
        //dbApp.verifyBeforeIndex("Student", new String[]{"id", "name", "gpa"});
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.updateTable(strTableName, "2024-01-01", htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(2));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(12));
//        htblColNameValue.put("name", new String("Dalia Noor"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(4));
//        htblColNameValue.put("name", new String("John Noor"));
//        htblColNameValue.put("gpa", new Double(1.5));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
        //dbApp.createIndex("Student" , new String[]{"id", "name", "gpa"});
        SQLTerm[] arrSQLTerms = new SQLTerm[3];
        arrSQLTerms[0] = new SQLTerm("Student", "id", "=", 2);
        arrSQLTerms[1] = new SQLTerm("Student", "name", ">", "Ahmed Noor");
        arrSQLTerms[2] = new SQLTerm("Student", "gpa", "=", 0.95);
        String[] strarrOperators = new String[2];
        strarrOperators[0] = "AND";
        strarrOperators[1] = "AND";
        SelectIterator si = (SelectIterator) dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        while(si.hasNext()){
            System.out.println(si.next());
        }
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(9));
//        htblColNameValue.put("name", new String("ZAKY NOOR"));
//        htblColNameValue.put("gpa", new Double(0.88));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(6));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(15));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(11));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(10));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(3));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(8));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(5));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(13));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(24));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(55));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(31));
//        htblColNameValue.put("name", new String("X"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(78));
//        htblColNameValue.put("name", new String("F"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(69));
//        htblColNameValue.put("name", new String("C"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(60));
//        htblColNameValue.put("name", new String("B"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(77));
//        htblColNameValue.put("name", new String("AN"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(34));
//        htblColNameValue.put("name", new String("A"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();

//        htblColNameValue.put("gpa", new Double(1.23));
//        dbApp.updateTable(strTableName, "78452", htblColNameValue);
//        dbApp.deleteFromTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(2343432));
//        dbApp.deleteFromTable(strTableName, htblColNameValue);

    }
}
