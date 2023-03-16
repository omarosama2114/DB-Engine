import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp {

    public void init(){};

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException, FileNotFoundException {

        //TODO check if the table already created and create folder whenever table is created.
        PrintWriter pw = new PrintWriter("meta-data.csv");
        for(String name: htblColNameType.keySet()){
            String type = htblColNameType.get(name);
            if(!htblColNameMax.containsKey(name)) {
                throw new DBAppException("No Such key in htbColNameMax");
            }
            if(!htblColNameMin.containsKey(name)){
                throw new DBAppException("No Such key in htbColNameMin");
            }
            pw.println(strTableName + "," + name + "," + type + "," + (strClusteringKeyColumn.equals(name)) + ",null,null," +
                    htblColNameMin.get(name) + "," + htblColNameMax.get(name));
        }
        pw.close();
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{

    }


    public boolean checkTypeRange(Object o, String min, String max) throws ParseException {
        if(o instanceof Integer){
            int converted = (int) o;
            int minConverted = Integer.parseInt(min), maxConverted = Integer.parseInt(max);
            return converted >= minConverted && converted <= maxConverted;
        }else if(o instanceof Double){
            double converted = (double) o;
            double minConverted = Double.parseDouble(min), maxConverted = Double.parseDouble(max);
            return !(converted < minConverted) && !(converted > maxConverted);
        }else if(o instanceof String){
            String converted  = (String) o;
            return converted.compareTo(min) >= 0 && converted.compareTo(max) <= 0;
        }else {
            Date converted = (Date) o;
            Date minConverted = new SimpleDateFormat("YYYY-MM-DD").parse(min);
            Date maxConverted = new SimpleDateFormat("YYYY-MM-DD").parse(max);
            return converted.compareTo(minConverted) >= 0 && converted.compareTo(maxConverted) <= 0;
        }
    }

    public void verifyBeforeInsert(String strTableName, Hashtable<String,Object> htblColNameValue)
            throws DBAppException, FileNotFoundException, ParseException {
        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while(sc.hasNext()){
            String[] splitted = sc.next().split(",");
            if(splitted[0].equals(strTableName)){
                cnt++;
                foundTable = true;
                if(!htblColNameValue.containsKey(splitted[1])){
                    throw new DBAppException("Table content does not match");
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if(!type.equals(splitted[2])){
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if(!checkTypeRange(currentObject, splitted[6], splitted[7])){
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        if(cnt != htblColNameValue.size()){
            throw new DBAppException("Input table does not match dimension of original table");
        }
        if(!foundTable){
            throw new DBAppException("No such table exist");
        }
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException, IOException, ParseException {

        verifyBeforeInsert(strTableName, htblColNameValue);
        //TODO
        /*
        String folderName = strTableName; // folder name to check
        String srcFolderPath = "src"; // path to the src folder
        File srcFolder = new File(srcFolderPath); // create a File object for the src folder
        if (srcFolder.isDirectory()) { // check if srcFolder is a directory
            File[] files = srcFolder.listFiles(); // get a list of files and folders in the src folder
            for (File file : files) { // iterate through the list of files and folders
                if (file.isDirectory() && file.getName().equals(folderName)) { // check if the current file is a folder with the specified name
                    System.out.println("Folder " + folderName + " exists in " + srcFolderPath);
                    return;
                }
            }
        }
         */

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
