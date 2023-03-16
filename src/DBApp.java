import java.io.*;
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
            pw.print(strTableName + "," + name + "," + type + "," + (strClusteringKeyColumn.equals(name)) + ",null,null," +
                    htblColNameMin.get(name) + "," + htblColNameMax.get(name));
        }
        pw.close();
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{

    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException, IOException {
        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        while(sc.hasNext()){
            String[] splitted = sc.next().split(",");
            if(splitted[0].equals(strTableName)){
                foundTable = true;
                //TODO validate data
                break;
            }
        }
        if(!foundTable){
            throw new DBAppException("No such table exist");
        }

        Properties prop = new Properties();
        String fileName = "DBApp.config";
        FileInputStream fileInputStream = new FileInputStream(fileName);
        prop.load(fileInputStream);
        int maxRow = Integer.parseInt(prop.getProperty("MaximumRowsCountingTablePage"));

    }

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
