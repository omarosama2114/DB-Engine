import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
public class DBApp {

    public void init(){};

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException, FileNotFoundException {
        PrintWriter pw = new PrintWriter("meta-data.csv");
        for(String name: htblColNameType.keySet()){
            String type = htblColNameType.get(name);
            if(!htblColNameMax.containsKey(name)) {
                throw new DBAppException("No Such key in htbColNameMax");
            }
            if(!htblColNameMin.containsKey(name)){
                throw new DBAppException("No Such key in htbColNameMin");
            }
            pw.print(strTableName + "," + name + "," + type + (strClusteringKeyColumn.equals(name)) + ",null,null," +
                    htblColNameMin.get(name) + "," + htblColNameMax.get(name));
        }
        pw.close();
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{}

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException{}

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue )
            throws DBAppException{}

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException{}

    /*public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException{}

        public Iterator parseSQL( StringBuffer strbufSQL ) throws
            DBAppException{}
     */



}
