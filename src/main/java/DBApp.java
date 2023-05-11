import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp{
    public void init() {
    }



    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException {

        File theDir = new File(strTableName);
        if (!theDir.exists()) {
            theDir.mkdirs();
        } else {
            throw new DBAppException("Table already exists");
        }
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileWriter("meta-data.csv", true));
            for (String name : htblColNameType.keySet()) {
                String type = htblColNameType.get(name);
                if (!htblColNameMax.containsKey(name)) {
                    throw new DBAppException("No Such key in htbColNameMax");
                }
                if (!htblColNameMin.containsKey(name)) {
                    throw new DBAppException("No Such key in htbColNameMin");
                }
                pw.println(strTableName + "," + name + "," + type + "," + (strClusteringKeyColumn.equals(name)) + ",null,null," +
                htblColNameMin.get(name) + "," + htblColNameMax.get(name));

            }
        } catch (IOException e) {
            throw new DBAppException("Failed to output data to the meta-data file.");
        }finally{
            if(pw != null)
                pw.close();
        }
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException {

    }


    public boolean checkTypeRange(Object o, String min, String max) throws ParseException {
        if (o instanceof Integer) {
            int converted = (int) o;
            int minConverted = Integer.parseInt(min), maxConverted = Integer.parseInt(max);
            return converted >= minConverted && converted <= maxConverted;
        } else if (o instanceof Double) {
            double converted = (double) o;
            double minConverted = Double.parseDouble(min), maxConverted = Double.parseDouble(max);
            return !(converted < minConverted) && !(converted > maxConverted);
        } else if (o instanceof String) {
            String converted = ((String) o).toLowerCase();
            min = min.toLowerCase();
            max = max.toLowerCase();
            return converted.compareTo(min) >= 0 && converted.compareTo(max) <= 0;
        } else {
            Date converted = (Date) o;
            Date minConverted = new SimpleDateFormat("yyyy-MM-dd").parse(min);
            Date maxConverted = new SimpleDateFormat("yyyy-MM-dd").parse(max);
            return converted.compareTo(minConverted) >= 0 && converted.compareTo(maxConverted) <= 0;
        }
    }

    public void verifyBeforeInsert(String strTableName, Hashtable<String, Object> htblColNameValue,
                                   StringBuilder clusteringKeyName)
            throws DBAppException, IOException, ParseException, ClassNotFoundException {
        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cntColumns = 0; // check equal columns
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                cntColumns++;
                foundTable = true;
                if (splitted[3].equals("true")) {
                    clusteringKeyName.append(splitted[1]);
                }
                if (!htblColNameValue.containsKey(splitted[1])) {
                    if(splitted[3].equals("true")){
                        sc.close();
                        throw new DBAppException("Clustering Key is missing");
                    }
                    htblColNameValue.put(splitted[1], new Null());
                    continue;
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                System.out.println(type + " " +splitted[2]);
                if (!type.equals(splitted[2])) {
                    sc.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if (!checkTypeRange(currentObject, splitted[6], splitted[7])) {
                    sc.close();
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        sc.close();
        if (!foundTable) {
            throw new DBAppException("No such table exist");
        }
        if(cntColumns < htblColNameValue.size()){
            throw new DBAppException("Columns Do not exist");
        }
        int[] boundaries = pagesBinarySearch(getTableSize(strTableName),
                (Comparable) htblColNameValue.get(clusteringKeyName.toString()), strTableName);
        if(boundaries[0] == boundaries[1]){
            Vector<SerializablePageRecord> tmp = readFromPage(strTableName, boundaries[0]).pageData;
            int pos = recordBinarySearch(tmp,
                    (Comparable) htblColNameValue.get(clusteringKeyName.toString()));
            if(tmp.get(pos).clusteringKey.compareTo(htblColNameValue.get(clusteringKeyName.toString())) == 0){
                throw new DBAppException("ClusteringKey already exists!");
            }
        }
    }


    public void createPage(String strTableName, int pageNumber, SerializablePageRecord a)
            throws IOException {
        Page page = new Page();
        page.addRecord(a);
        writeToPage(strTableName, page, pageNumber);
    }

    public void writeToPage(String strTableName, Page page, int pageNumber) throws IOException {
        FileOutputStream fos = new FileOutputStream(strTableName + "/" + pageNumber + ".class");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(page);
        fos.close();
        oos.close();
    }

    public Page readFromPage(String strTableName, int pageNum) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(strTableName + "/" + pageNum + ".class");
        ObjectInputStream ois = new ObjectInputStream(fis);
        Page b = (Page) ois.readObject();
        ois.close();
        fis.close();
        return b;
    }

    public int recordBinarySearch(Vector<SerializablePageRecord> records, Comparable clusteringKey)
            throws IOException, ClassNotFoundException {
        int low = 0, high = records.size() - 1, ans = -1;
        while (low <= high) {
            int mid = (low + high) / 2;
            if (records.get(mid).clusteringKey.compareTo(clusteringKey) < 0) {
                low = mid + 1;
            } else {
                ans = mid;
                high = mid - 1;
            }
        }
        return ans;
    }

    public void shift(SerializablePageRecord spr, String strTableName, int pageNum,
                      int maxPages) throws IOException, ClassNotFoundException {
        Page p = readFromPage(strTableName, pageNum);
        Vector<SerializablePageRecord> records = p.pageData;
        int ans = recordBinarySearch(records, spr.clusteringKey);
        if (ans == -1) {
            records.add(spr);
        } else
            records.insertElementAt(spr, ans);
        writeToPage(strTableName, p, pageNum);
        int idx = pageNum;
        SerializablePageRecord sprTmp = null;
        while (idx <= maxPages) {
            Page currPage = readFromPage(strTableName, idx);
            if (sprTmp != null) {
                currPage.pageData.insertElementAt(sprTmp, 0);
            }
            if (currPage.pageData.size() <= currPage.maxRows) {
                sprTmp = null;
                writeToPage(strTableName, currPage, idx);
                break;
            }
            sprTmp = currPage.pageData.remove(currPage.pageData.size() - 1);
            writeToPage(strTableName, currPage, idx);
            idx++;
        }
        if (sprTmp != null) {
            createPage(strTableName, idx, sprTmp);
        }
    }

    public SerializablePageRecord createRecord(Hashtable<String, Object> htblColNameValue,
                                               Comparable clusteringKey) {
        SerializablePageRecord a = new SerializablePageRecord(htblColNameValue, clusteringKey);
        return a;
    }

    public void insertionHandler(int[] boundaries, int pagesCount, String strTableName,
                                 Hashtable<String, Object> htblColNameValue,
                                 StringBuilder clusteringKeyName)
            throws IOException, ClassNotFoundException {
        int first = boundaries[0];
        int second = boundaries[1];
        Comparable clusteringKey = (Comparable) htblColNameValue.get(clusteringKeyName.toString());
        SerializablePageRecord a = createRecord(htblColNameValue, clusteringKey);
        if (pagesCount == 0) {
            createPage(strTableName, 1, a);
        } else if (second == pagesCount + 1) {
            Page b = readFromPage(strTableName, first);
            if (b.maxRows == b.pageData.size()) {
                createPage(strTableName, first + 1, a);
            } else {
                b.addRecord(a);
                writeToPage(strTableName, b, first);
            }
        } else {
            if (first == 0) first++;
            shift(a, strTableName, first, pagesCount);
        }
    }

    public int[] pagesBinarySearch(int pagesCount, Comparable id, String strTableName)
            throws IOException, ClassNotFoundException {
        int low = 1;
        int high = pagesCount;
        int targetPage = -1;
        while (low <= high) {
            int mid = (low + high) / 2;
            Page b = readFromPage(strTableName, mid);
            Comparable min = b.pageData.get(0).clusteringKey;
            Comparable max = b.pageData.get(b.pageData.size() - 1).clusteringKey;
            if (id.compareTo(min) < 0) {
                high = mid - 1;
            } else if (id.compareTo(max) > 0) {
                low = mid + 1;
            } else {
                low = mid;
                high = mid;
                break;
            }
        }

        return new int[]{high, low};
    }

    public int getTableSize(String strTableName) {
        File tableFolder = new File(strTableName);
        String[] fileNames = tableFolder.list();
        assert fileNames != null;
        int pagesCount = fileNames.length;
        return pagesCount;
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        StringBuilder clusteringKeyName = new StringBuilder();
        try {
            verifyBeforeInsert(strTableName, htblColNameValue, clusteringKeyName);
            int pagesCount = getTableSize(strTableName);
            int[] boundaries = pagesBinarySearch(pagesCount, (Comparable) htblColNameValue.get(clusteringKeyName.toString()), strTableName);
            insertionHandler(boundaries, pagesCount, strTableName, htblColNameValue, clusteringKeyName);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException e) {
            throw new DBAppException("Problem with IO happened.");
        } catch (ParseException e) {
            throw new DBAppException("Parse exception occurred.");
        }

    }

    public void verifyBeforeUpdate(String strTableName, Hashtable<String, Object> htblColNameValue,
                                   Object clusteringKeyValue)
            throws DBAppException, FileNotFoundException, ParseException {

        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                if (!splitted[3].equals("true"))
                    cnt++;
                else continue;
                foundTable = true;
                if (!htblColNameValue.containsKey(splitted[1])) {
                    sc.close();
                    throw new DBAppException("Table content does not match");
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if (!type.equals(splitted[2])) {
                    sc.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if (!checkTypeRange(currentObject, splitted[6], splitted[7])) {
                    sc.close();
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        sc.close();
        if (cnt != htblColNameValue.size()) {
            throw new DBAppException("Input table does not match dimension of original table");
        }
        if (!foundTable) {
            throw new DBAppException("No such table exist");
        }
    }

    public Object getKey(String clusteringKeyValue, String strTableName) throws FileNotFoundException, ParseException, DBAppException {
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                if (splitted[3].equals("true")){
                    String now = splitted[2];
                    if(now.contains("Double")){
                        return Double.parseDouble(clusteringKeyValue);
                    }else if(now.contains("Integer")){
                        return Integer.parseInt(clusteringKeyValue);
                    }else if(now.contains("String")){
                        return clusteringKeyValue;
                    }else{
                        return new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
                    }
                }
            }
        }
        sc.close();
        throw new DBAppException("Clustering Key Parse problem");
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            int pagesCount = getTableSize(strTableName);
            Comparable clusteringKey = (Comparable) getKey(strClusteringKeyValue, strTableName);
            verifyBeforeUpdate(strTableName, htblColNameValue, clusteringKey);
            int[] boundaries = pagesBinarySearch(pagesCount, clusteringKey, strTableName);
            if(boundaries[0] == 0) {
                throw new DBAppException("Clustering Key does not exist");
            }
            Page p = readFromPage(strTableName, boundaries[0]);
            int idx = recordBinarySearch(p.pageData, clusteringKey);
            SerializablePageRecord spr = p.pageData.get(idx);
            if (spr.clusteringKey.compareTo(clusteringKey) != 0)
                throw new DBAppException("Clustering Key does not exist");
            for(String key : htblColNameValue.keySet())
                spr.recordHash.put(key, htblColNameValue.get(key));
            writeToPage(strTableName, p, boundaries[0]);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException e) {
            throw new DBAppException("Problem with IO happened.");
        } catch (ParseException e) {
            throw new DBAppException("Parse exception occurred.");
        }
    }

    public String getClusteringKeyName(String strTableName, Hashtable<String, Object> htblColNameValue) throws FileNotFoundException, DBAppException {
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        HashSet<String> columns = new HashSet<>();
        String temp = "";
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                columns.add(splitted[1]);
                if (splitted[3].equals("true")){
                    temp = splitted[1];
                }
            }
        }
        sc.close();
        if(columns.isEmpty()) throw new DBAppException("Table Not Found Or Table with No Clustering Key");
        for(String key: htblColNameValue.keySet()){
            if(!columns.contains(key)){
                throw new DBAppException("Column does not exist");
            }
        }
        return temp;
    }


    public void deleteEntirePage(String strTableName, Vector<Integer> pagesIdx) throws IOException, ClassNotFoundException {
        for(int idx: pagesIdx){
            Files.delete(Paths.get(strTableName + "/" + idx + ".class"));
        }
    }

    public void renameFile(String strTableName, int oldIdx, int newIdx) {
        File oldFile = new File(strTableName + "/" + oldIdx + ".class");
        File newFile = new File(strTableName + "/" + newIdx + ".class");
        oldFile.renameTo(newFile);
    }
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {
            String clusteringKeyName = getClusteringKeyName(strTableName, htblColNameValue);
            int pagesCount = getTableSize(strTableName);
            Vector<Integer> toBeDeleted = new Vector<>();
            HashSet<Integer> found = new HashSet<>();

            if(htblColNameValue.isEmpty()){
                int size = getTableSize(strTableName);
                for(int i = 1; i<=size; i++) {
                    toBeDeleted.add(i);
                }
            }else if(htblColNameValue.containsKey(clusteringKeyName)) {
                    int[] boundaries = pagesBinarySearch(pagesCount, (Comparable) htblColNameValue.get(clusteringKeyName), strTableName);
                    boolean matched = true;
                    if (boundaries[0] != boundaries[1]) {
                        matched = false;
                        return;
                    }
                    
                    Page currentPage = readFromPage(strTableName, boundaries[0]);
                    int idx = recordBinarySearch(currentPage.pageData, (Comparable) htblColNameValue.get(clusteringKeyName));
                    if (idx == -1 || currentPage.maxRows <= idx || !currentPage.pageData.get(idx).clusteringKey.equals(htblColNameValue.get(clusteringKeyName))) {
                        matched = false;
                        return;
                    }
                    
                    SerializablePageRecord currentRecord = currentPage.pageData.get(idx);
                    for (String key : currentRecord.recordHash.keySet()) {
                        if (htblColNameValue.containsKey(key)
                                && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                            matched = false;
                            return;
                        }
                    }
                    if(matched) {
                        currentPage.pageData.remove(idx);
                    }
                    
                    if(currentPage.pageData.isEmpty()){
                        toBeDeleted.add(boundaries[0]);
                        found.add(boundaries[0]);
                    }else{
                    writeToPage(strTableName, currentPage, boundaries[0]);
                }
                
            }else {
                for(int i = 1; i<=pagesCount; i++){
                    Page currentPage = readFromPage(strTableName, i);
                    for(SerializablePageRecord currentRecord: (Vector<SerializablePageRecord>) currentPage.pageData.clone()){
                        boolean matched = true;
                        for (String key : currentRecord.recordHash.keySet()) {
                            if (htblColNameValue.containsKey(key)
                                    && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                                matched = false;
                            }
                        }
                        if(matched){
                            currentPage.pageData.remove(currentRecord);
                        }
                    }
                    if(currentPage.pageData.isEmpty()){
                        toBeDeleted.add(i);
                        found.add(i);
                    }else{
                        writeToPage(strTableName, currentPage, i);
                    }
                }
            }
            deleteEntirePage(strTableName, toBeDeleted);
            int cnt = 0;
            for(int i = 1; i<=pagesCount; i++){
                if(found.contains(i)){
                    cnt++;
                }else if(cnt > 0){
                    renameFile(strTableName, i, i - cnt);
                }
            }
        
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException e) {
            throw new DBAppException("Problem with IO happened.");
        }
        
        
    }
    
    /*public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
    String[] strarrOperators)
    throws DBAppException{}
    
    public Iterator parseSQL( StringBuffer strbufSQL ) throws
    DBAppException{}
    */
    
    
    public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {
//        String strTableName = "Student";
//        DBApp dbApp = new DBApp( );
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        Hashtable htblColNameMin = new Hashtable();
//        htblColNameMin.put("id", "0");
//        htblColNameMin.put("name", "A");
//        htblColNameMin.put("gpa", "0");
//        Hashtable htblColNameMax = new Hashtable();
//        htblColNameMax.put("id", "100000000");
//        htblColNameMax.put("name", "ZZZZZZZZZZZZZZZZZ");
//        htblColNameMax.put("gpa", "10000000");
//        dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 2343432 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 453455 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 5674567 ));
//        htblColNameValue.put("name", new String("Dalia Noor" ) );
//        htblColNameValue.put("gpa", new Double( 1.25 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 23498 ));
//        htblColNameValue.put("name", new String("John Noor" ) );
//        htblColNameValue.put("gpa", new Double( 1.5 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 78452 ));
//        htblColNameValue.put("name", new String("ZAKY NOOR" ) );
//        htblColNameValue.put("gpa", new Double( 0.88 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear();
//        htblColNameValue.put("name", new String("ZAKY NOOR" ));
//        htblColNameValue.put("gpa", new Double( 1.23));
//        dbApp.updateTable(strTableName, "78452", htblColNameValue);
    }

}
