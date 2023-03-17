import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp {
    //TODO TEST THE FUNCTIONALITIES
    public void init(){};

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException, IOException {

        File theDir = new File(strTableName);
        if (!theDir.exists()){
            theDir.mkdirs();
        }
        else {
            throw new DBAppException("Table already exists");
        }
        PrintWriter pw = new PrintWriter(new FileWriter("meta-data.csv", true));
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
                            String[] strarrColName) throws DBAppException {

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

    public void verifyBeforeInsert(String strTableName, Hashtable<String,Object> htblColNameValue,
                                   Vector<String> keys, StringBuilder clusteringKeyName)
            throws DBAppException, FileNotFoundException, ParseException {
        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while(sc.hasNext()){
            String[] splitted = sc.next().split(",");
            if(splitted[0].equals(strTableName)){
                cnt++;
                foundTable = true;
                keys.add(splitted[1]);
                if(splitted[3].equals("true")) {
                    clusteringKeyName = new StringBuilder(splitted[1]);
                }
                if(!htblColNameValue.containsKey(splitted[1])){
                    sc.close();
                    throw new DBAppException("Table content does not match");
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if(!type.equals(splitted[2])){
                    sc.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if(!checkTypeRange(currentObject, splitted[6], splitted[7])){
                    sc.close();
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        sc.close();
        if(cnt != htblColNameValue.size()){
            throw new DBAppException("Input table does not match dimension of original table");
        }
        if(!foundTable){
            throw new DBAppException("No such table exist");
        }
    }

    public void createPage(String strTableName, Hashtable<String,Object> htblColNameValue, Vector<String> keys,
                           StringBuilder clusteringKeyName, int pageNumber, SerializablePageRecord a)
                           throws IOException {
        Page page = new Page();
        page.addRecord(a);
        writeToPage(strTableName, page, pageNumber);
    }

    public void writeToPage(String strTableName, Page page, int pageNumber) throws IOException {
        FileOutputStream fos = new FileOutputStream(strTableName + "/" + pageNumber + ".class");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(page);
        oos.close();
    }

    public void insertionHandler(int[] boundaries, int pagesCount, String strTableName,
                                 Hashtable<String,Object> htblColNameValue, Vector<String> keys,
                                 StringBuilder clusteringKeyName)
                                 throws IOException, ClassNotFoundException {
        int first = boundaries[0];
        int second = boundaries[1];
        int clusteringKey = (int) htblColNameValue.get(clusteringKeyName.toString());
        StringBuilder record = new StringBuilder();
        for(String s : keys) {
            record.append(htblColNameValue.get(s));
            record.append(",");
        }
        record.deleteCharAt(record.length() - 1);
        SerializablePageRecord a = new SerializablePageRecord(record.toString() , clusteringKey);
        if(pagesCount == 0) {
            createPage(strTableName, htblColNameValue, keys, clusteringKeyName, 1 , a);
        }
        else if(second == pagesCount + 1) {
            FileInputStream fis = new FileInputStream(strTableName + "/" + first + ".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            Page b = (Page) ois.readObject();

            if(b.maxRows == b.pageData.size()) {
                createPage(strTableName, htblColNameValue, keys, clusteringKeyName, first + 1, a);
            }
            else {
                b.addRecord(a);
                writeToPage(strTableName, b, first);
            }
        }

    }

    public int[] binarySearch(int pagesCount, int id, String strTableName)
            throws IOException, ClassNotFoundException {
        int low = 1;
        int high = pagesCount;
        int targetPage = -1;
        while(low <= high) {
            int mid = (low + high) / 2;
            FileInputStream fis = new FileInputStream(strTableName + "/" + mid + ".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            Page b = (Page) ois.readObject();
            int min = b.pageData.get(0).clusteringKey;
            int max = b.pageData.get(b.pageData.size() - 1).clusteringKey;
            if(id < min) {
                high = mid - 1;
            }
            else if(id > max) {
                low = mid + 1;
            }
            else {
                low = mid;
                high = mid;
                break;
            }
            // closing stream
            ois.close();
            fis.close();
        }

        return new int[]{high, low};
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException, IOException, ParseException {

        Vector<String> keys = new Vector<>();
        StringBuilder clusteringKeyName = new StringBuilder();
        verifyBeforeInsert(strTableName, htblColNameValue, keys, clusteringKeyName);
        File tableFolder = new File(strTableName);
        if (tableFolder.isDirectory()) {
            String[] fileNames = tableFolder.list();
            int pagesCount = fileNames.length;
        }



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
