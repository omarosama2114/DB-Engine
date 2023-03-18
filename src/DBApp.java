import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp {
    //TODO TEST THE FUNCTIONALITIES
    public void init() {
    }

    ;

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException, IOException {

        File theDir = new File(strTableName);
        if (!theDir.exists()) {
            theDir.mkdirs();
        } else {
            throw new DBAppException("Table already exists");
        }
        PrintWriter pw = new PrintWriter(new FileWriter("meta-data.csv", true));
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
        pw.close();
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
            String converted = (String) o;
            return converted.compareTo(min) >= 0 && converted.compareTo(max) <= 0;
        } else {
            Date converted = (Date) o;
            Date minConverted = new SimpleDateFormat("YYYY-MM-DD").parse(min);
            Date maxConverted = new SimpleDateFormat("YYYY-MM-DD").parse(max);
            return converted.compareTo(minConverted) >= 0 && converted.compareTo(maxConverted) <= 0;
        }
    }

    public void verifyBeforeInsert(String strTableName, Hashtable<String, Object> htblColNameValue,
                                   Vector<String> keys, StringBuilder clusteringKeyName)
            throws DBAppException, FileNotFoundException, ParseException {
        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                cnt++;
                foundTable = true;
                keys.add(splitted[1]);
                if (splitted[3].equals("true")) {
                    clusteringKeyName.append(splitted[1]);
                }
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

    public int recordBinarySearch(Vector<SerializablePageRecord> records, int clusteringKey)
            throws IOException, ClassNotFoundException {
        int low = 0, high = records.size() - 1, ans = -1;
        while (low <= high) {
            int mid = (low + high) / 2;
            if (records.get(mid).clusteringKey < clusteringKey) {
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
                                               int clusteringKey, Vector<String> keys) {
        StringBuilder record = new StringBuilder();
        for (String s : keys) {
            record.append(htblColNameValue.getOrDefault(s, clusteringKey));
            record.append(",");
        }
        record.deleteCharAt(record.length() - 1);
        SerializablePageRecord a = new SerializablePageRecord(record.toString(), clusteringKey);
        return a;
    }

    public void insertionHandler(int[] boundaries, int pagesCount, String strTableName,
                                 Hashtable<String, Object> htblColNameValue, Vector<String> keys,
                                 StringBuilder clusteringKeyName)
            throws IOException, ClassNotFoundException {
        int first = boundaries[0];
        int second = boundaries[1];
        int clusteringKey = (int) htblColNameValue.get(clusteringKeyName.toString());
        SerializablePageRecord a = createRecord(htblColNameValue, clusteringKey, keys);
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

    public int[] pagesBinarySearch(int pagesCount, int id, String strTableName)
            throws IOException, ClassNotFoundException {
        int low = 1;
        int high = pagesCount;
        int targetPage = -1;
        while (low <= high) {
            int mid = (low + high) / 2;
            Page b = readFromPage(strTableName, mid);
            int min = b.pageData.get(0).clusteringKey;
            int max = b.pageData.get(b.pageData.size() - 1).clusteringKey;
            if (id < min) {
                high = mid - 1;
            } else if (id > max) {
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
            throws DBAppException, IOException, ParseException, ClassNotFoundException {
        Vector<String> keys = new Vector<>();
        StringBuilder clusteringKeyName = new StringBuilder();
        verifyBeforeInsert(strTableName, htblColNameValue, keys, clusteringKeyName);
        int pagesCount = getTableSize(strTableName);
        int[] boundaries = pagesBinarySearch(pagesCount, (int) htblColNameValue.get(clusteringKeyName.toString()), strTableName);
        insertionHandler(boundaries, pagesCount, strTableName, htblColNameValue, keys, clusteringKeyName);

    }

    public void verifyBeforeUpdate(String strTableName, Hashtable<String, Object> htblColNameValue,
                                   Vector<String> keys, int clusteringKeyValue)
            throws DBAppException, FileNotFoundException, ParseException {

        boolean foundTable = false;
        Scanner sc = new Scanner(new FileReader("meta-data.csv"));
        int cnt = 0;
        while (sc.hasNext()) {
            String[] splitted = sc.next().split(",");
            if (splitted[0].equals(strTableName)) {
                if (!splitted[3].equals("true"))
                    cnt++;
                foundTable = true;
                keys.add(splitted[1]);
                if (!htblColNameValue.containsKey(splitted[1]) && splitted[3].equals("false")) {
                    sc.close();
                    throw new DBAppException("Table content does not match");
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if (!type.equals(splitted[2]) && splitted[3].equals("false")) {
                    sc.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = (splitted[3].equals("true") ? clusteringKeyValue : htblColNameValue.get(splitted[1]));
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


    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        int pagesCount = getTableSize(strTableName);
        Vector<String> keys = new Vector<>();
        int clusteringKey = Integer.parseInt(strClusteringKeyValue);
        verifyBeforeUpdate(strTableName, htblColNameValue, keys, clusteringKey);
        int[] boundaries = pagesBinarySearch(pagesCount, clusteringKey, strTableName);
        Page p = readFromPage(strTableName, boundaries[0]);
        int idx = recordBinarySearch(p.pageData, clusteringKey);
        SerializablePageRecord spr = p.pageData.get(idx);
        if (spr.clusteringKey != clusteringKey)
            throw new DBAppException("Clustering Key does not exist");
        SerializablePageRecord newRecord = createRecord(htblColNameValue, clusteringKey, keys);
        p.pageData.set(idx, newRecord);
        writeToPage(strTableName, p, boundaries[0]);
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
    }

    /*public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException{}

        public Iterator parseSQL( StringBuffer strbufSQL ) throws
            DBAppException{}
     */


}
