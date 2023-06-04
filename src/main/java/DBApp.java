import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp{

//TODO Complete DBApp
    static HashMap<String, Octree> openedBefore = new HashMap<>();

    public void init() {
    }

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
        CSVWriter pw = null;
        try {
            File f = new File("meta-data.csv");
            Vector<String[]> toBePrinted = new Vector<>();
            if(f.exists() && !f.isDirectory()) {
                FileReader fileReader = new FileReader("meta-data.csv");
                CSVReader csvReader = new CSVReader(fileReader);
                List<String[]> allData = csvReader.readAll();
                toBePrinted.addAll(allData);
            }
            pw = new CSVWriter(new FileWriter("meta-data.csv"));
            for (String name : htblColNameType.keySet()) {
                String type = htblColNameType.get(name);
                if (!htblColNameMax.containsKey(name)) {
                    throw new DBAppException("No Such key in htbColNameMax");
                }
                if (!htblColNameMin.containsKey(name)) {
                    throw new DBAppException("No Such key in htbColNameMin");
                }
                toBePrinted.add(new String[]{strTableName, name, type, "" + (strClusteringKeyColumn.equals(name)), "null",
                        "null",htblColNameMin.get(name), htblColNameMax.get(name)});
            }
            pw.writeAll(toBePrinted);
        } catch (IOException e) {
            throw new DBAppException("Failed to output data to the meta-data file.");
        } catch (CsvException e) {
            throw new RuntimeException(e);
        } finally{
            if(pw != null)
                pw.close();
        }
    }

    public void verifyBeforeIndex
            (String strTableName, String[] strarrColName, Vector<Comparable> min, Vector<Comparable> max ) throws DBAppException, IOException, CsvException, ParseException {
        boolean foundTable = false;
        File inputFile = new File("meta-data.csv");
        // Read existing file
        CSVReader reader = new CSVReader(new FileReader(inputFile));
        List<String[]> csvBody = reader.readAll();
        HashMap<String, Boolean> columns = new HashMap<>();
        for(String[] splitted: csvBody) {
            if (splitted[0].equals(strTableName)) {
                foundTable = true;
                columns.put(splitted[1], !splitted[4].equals("null"));
            }
        }
        if (!foundTable) {
            reader.close();
            throw new DBAppException("No such table exist");
        }
        HashSet<String> count = new HashSet<>(List.of(strarrColName));
        if(count.size() != 3){
            reader.close();
            throw new DBAppException("Index must be created on 3 different columns");
        }
        for(String str: strarrColName){
            if(!columns.containsKey(str) || columns.get(str)){
                reader.close();
                throw new DBAppException("Column not found or index already created on columns");
            }
        }

        // Write to CSV file which is open
        CSVWriter writer = new CSVWriter(new FileWriter(inputFile));
        String indexName = strarrColName[0] + "_" + strarrColName[1] + "_" + strarrColName[2];
        Hashtable<String , Object> minValues = new Hashtable<>() , maxValues = new Hashtable<>();
        for(String[] x: csvBody) {
            if(Objects.equals(x[0], strTableName) && count.contains(x[1])){
                x[4] = indexName;
                x[5] = "OCTree";
                if(x[2].equals("java.lang.Integer")) {
                   minValues.put(x[1] , Integer.parseInt(x[6]));
                   maxValues.put(x[1] , Integer.parseInt(x[7]));

                }
                else if(x[2].equals("java.lang.Double")) {
                    minValues.put(x[1] , Double.parseDouble(x[6]));
                    maxValues.put(x[1] ,Double.parseDouble(x[7]));
                }
                else if(x[2].equals("java.lang.String")) {
                    minValues.put(x[1] , x[6]);
                    maxValues.put(x[1] , x[7]);
                }
                else if(x[2].equals("java.util.Date")) {
                    minValues.put(x[1] , new SimpleDateFormat("yyyy-MM-dd").parse(x[6]));
                    maxValues.put(x[1] , new SimpleDateFormat("yyyy-MM-dd").parse(x[7]));
                }
            }
        }
        for(String s : strarrColName){
            min.add((Comparable) minValues.get(s));
            max.add((Comparable) maxValues.get(s));
        }
        writer.writeAll(csvBody);
        writer.flush();
        writer.close();
        reader.close();
    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        try {
            Vector<Comparable> colMin = new Vector<>();
            Vector<Comparable> colMax = new Vector<>();
            verifyBeforeIndex(strTableName, strarrColName, colMin, colMax);
            Octree octree = new Octree(colMin.get(0), colMin.get(1) , colMin.get(2) , colMax.get(0) , colMax.get(1) , colMax.get(2));
            int size = getTableSize(strTableName);
            for(int i = 1; i <= size; i++) {
                Page p = readFromPage(strTableName, i);
                for(int j = 0; j < p.pageData.size(); j++) {
                    SerializablePageRecord spr = p.pageData.get(j);
                    RecordReference r = new RecordReference(i+"", spr.clusteringKey);
                    Vector<Comparable> temp = new Vector<>();
                    for(int k = 0; k < strarrColName.length; k++) {
                        temp.add((Comparable) spr.recordHash.get(strarrColName[k]));
                    }
                    octree.insert(temp.get(0), temp.get(1), temp.get(2), r);
                }
            }
            String indexName = strarrColName[0] + "-" + strarrColName[1] + "-" + strarrColName[2];
            File theDir = new File(strTableName+"_index");
            if (!theDir.exists()) {
                theDir.mkdirs();
            }
            writeToOctree(octree, strTableName, indexName);

        } catch (IOException | CsvException | ParseException | ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
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
            throws DBAppException, IOException, ParseException, ClassNotFoundException, CsvException {
        boolean foundTable = false;
        File inputFile = new File("meta-data.csv");
        CSVReader reader = new CSVReader(new FileReader(inputFile));
        List<String[]> csvBody =reader.readAll();
        int cntColumns = 0; // check equal columns
        for(String[] splitted: csvBody) {
            if (splitted[0].equals(strTableName)) {
                cntColumns++;
                foundTable = true;
                if (splitted[3].equals("true")) {
                    clusteringKeyName.append(splitted[1]);
                }
                if (!htblColNameValue.containsKey(splitted[1])) {
                    if(splitted[3].equals("true")){
                        reader.close();
                        throw new DBAppException("Clustering Key is missing");
                    }
                    throw new DBAppException("Column is missing");

                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if (!type.equals(splitted[2])) {
                    reader.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if (!checkTypeRange(currentObject, splitted[6], splitted[7])) {
                    reader.close();
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        reader.close();
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

    public void writeToOctree(Octree tree, String strTableName, String name) throws IOException {
        FileOutputStream fos = new FileOutputStream(strTableName + "_index/" + name + ".class");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(tree);
        fos.close();
        oos.close();
    }

    public Octree readFromOctree(String strTableName, String name) throws IOException, ClassNotFoundException {
        String file = strTableName + "_index/" + name + ".class";
        if(openedBefore.containsKey(file))return openedBefore.get(file);
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Octree b = (Octree) ois.readObject();
        ois.close();
        fis.close();
        openedBefore.put(file, b);
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
        RecordReference rr = new RecordReference(pageNum+"", spr.clusteringKey);
        int ans = recordBinarySearch(records, spr.clusteringKey);
        if (ans == -1)
            records.add(spr);
        else
            records.insertElementAt(spr, ans);
        insertIntoAllOctrees(strTableName, spr.recordHash, rr);
        writeToPage(strTableName, p, pageNum);
        int idx = pageNum;
        SerializablePageRecord sprTmp = null;
        while (idx <= maxPages) {
            Page currPage = readFromPage(strTableName, idx);
            if (sprTmp != null) {
                currPage.pageData.insertElementAt(sprTmp, 0);
                updateInAllOctrees(strTableName, sprTmp, idx, new Hashtable<>());
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
            updateInAllOctrees(strTableName, sprTmp, idx, new Hashtable<>());
        }
    }

    //TODO
    //Rename record oldname to newname in all octrees
    public void renameInAllOctrees(HashMap<String, String> name, String strTableName) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return;
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                Octree tree = readFromOctree(strTableName, tmp[0]);
                tree.rename(name, tree.root);
                writeToOctree(tree, strTableName, tmp[0]);
            }
        }
    }

    public void updateInAllOctrees(String strTableName, SerializablePageRecord srp, int idx, Hashtable<String, Object> newData) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return;
        RecordReference rr = new RecordReference(idx+"", srp.clusteringKey);
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                String[] tmp2 = tmp[0].split("-");
                Comparable x = (Comparable) srp.recordHash.get(tmp2[0]);
                Comparable y = (Comparable) srp.recordHash.get(tmp2[1]);
                Comparable z = (Comparable) srp.recordHash.get(tmp2[2]);
                Comparable newX = (Comparable) newData.getOrDefault(tmp2[0], x);
                Comparable newY = (Comparable) newData.getOrDefault(tmp2[1], y);
                Comparable newZ = (Comparable) newData.getOrDefault(tmp2[2], z);
                Octree tree = readFromOctree(strTableName, tmp[0]);
                tree.update(x, y, z, newX, newY, newZ, rr);
                writeToOctree(tree, strTableName, tmp[0]);
            }
        }
    }

    public void deleteFromAllOctrees(String strTableName, SerializablePageRecord spr) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return;
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                String[] tmp2 = tmp[0].split("-");
                Comparable x = (Comparable) spr.recordHash.get(tmp2[0]);
                Comparable y = (Comparable) spr.recordHash.get(tmp2[1]);
                Comparable z = (Comparable) spr.recordHash.get(tmp2[2]);
                Octree tree = readFromOctree(strTableName, tmp[0]);
                tree.remove(x, y, z, spr.clusteringKey);
                writeToOctree(tree, strTableName, tmp[0]);
            }
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
        RecordReference r = null;
        if (pagesCount == 0) {
            r = new RecordReference("1", 0);
            insertIntoAllOctrees(strTableName, htblColNameValue, r);
            createPage(strTableName, 1, a);
        } else if (second == pagesCount + 1) {
            Page b = readFromPage(strTableName, first);
            if (b.maxRows == b.pageData.size()) {
                r = new RecordReference(first + 1 + "", 0);
                insertIntoAllOctrees(strTableName, htblColNameValue, r);
                createPage(strTableName, first + 1, a);
            } else {
                r = new RecordReference(first + "", b.pageData.size());
                insertIntoAllOctrees(strTableName, htblColNameValue, r);
                b.addRecord(a);
                writeToPage(strTableName, b, first);
            }
        } else {
            if (first == 0) first++;
            shift(a, strTableName, first, pagesCount);
        }
    }

    public void insertIntoAllOctrees
            (String strTableName, Hashtable<String, Object> htblColNameValue, RecordReference r) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return;
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                String[] tmp2 = tmp[0].split("-");

                Comparable x = (Comparable) htblColNameValue.get(tmp2[0]);
                Comparable y = (Comparable) htblColNameValue.get(tmp2[1]);
                Comparable z = (Comparable) htblColNameValue.get(tmp2[2]);
                Octree tree = readFromOctree(strTableName, tmp[0]);
                tree.insert(x, y, z, r);
                writeToOctree(tree, strTableName, tmp[0]);
            }
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
            for(String key: htblColNameValue.keySet()){
                Object value = htblColNameValue.get(key);
                if(value instanceof String){
                    value = ((String) value).toLowerCase();
                    htblColNameValue.put(key, value);
                }
            }
            verifyBeforeInsert(strTableName, htblColNameValue, clusteringKeyName);
            int pagesCount = getTableSize(strTableName);
            int[] boundaries = pagesBinarySearch(pagesCount, (Comparable) htblColNameValue.get(clusteringKeyName.toString()), strTableName);
            insertionHandler(boundaries, pagesCount, strTableName, htblColNameValue, clusteringKeyName);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException | CsvException e) {
            throw new DBAppException("Problem with IO happened.");
        } catch (ParseException e) {
            throw new DBAppException("Parse exception occurred.");
        }
    }

    public void verifyBeforeUpdate(String strTableName, Hashtable<String, Object> htblColNameValue,
                                   Object clusteringKeyValue)
            throws DBAppException, IOException, ParseException, CsvException {

        boolean foundTable = false;
        File file = new File("meta-data.csv");
        CSVReader csvReader = new CSVReader(new FileReader(file));
        List<String[]> colNames = csvReader.readAll();
        int cnt = 0;
        for(String[] splitted : colNames) {
            if (splitted[0].equals(strTableName)) {
                if (!splitted[3].equals("true"))
                    cnt++;
                else continue;
                foundTable = true;
                if (!htblColNameValue.containsKey(splitted[1])) {
                    csvReader.close();
                    throw new DBAppException("Table content does not match");
                }
                String type = htblColNameValue.get(splitted[1]).getClass().toString().substring(6);
                if (!type.equals(splitted[2])) {
                    csvReader.close();
                    throw new DBAppException("Type Mismatch Error");
                }
                Object currentObject = htblColNameValue.get(splitted[1]);
                if (!checkTypeRange(currentObject, splitted[6], splitted[7])) {
                    csvReader.close();
                    throw new DBAppException("Ranges Exceeded Error");
                }
            }
        }
        csvReader.close();
        if (cnt != htblColNameValue.size()) {
            throw new DBAppException("Input table does not match dimension of original table");
        }
        if (!foundTable) {
            throw new DBAppException("No such table exist");
        }
    }

    public Object getKey(String clusteringKeyValue, String strTableName) throws IOException, ParseException, DBAppException, CsvException {
        File file = new File("meta-data.csv");
        CSVReader csvReader = new CSVReader(new FileReader(file));
        List<String[]> colNames = csvReader.readAll();
        int cnt = 0;
        for(String[] splitted : colNames) {
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
        csvReader.close();
        throw new DBAppException("Clustering Key Parse problem");
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            for(String key: htblColNameValue.keySet()){
                Object value = htblColNameValue.get(key);
                if(value instanceof String){
                    value = ((String) value).toLowerCase();
                    htblColNameValue.put(key, value);
                }
            }
            strClusteringKeyValue = strClusteringKeyValue.toLowerCase();
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
            updateInAllOctrees(strTableName, spr, boundaries[0], htblColNameValue);
            for(String key : htblColNameValue.keySet()) {
                spr.recordHash.put(key, htblColNameValue.get(key));
            }
            writeToPage(strTableName, p, boundaries[0]);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException e) {
            throw new DBAppException("Problem with IO happened.");
        } catch (ParseException e) {
            throw new DBAppException("Parse exception occurred.");
        } catch (CsvException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public String getClusteringKeyName(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException, CsvException {
        File file = new File("meta-data.csv");
        CSVReader csvReader = new CSVReader(new FileReader(file));
        List<String[]> colNames = csvReader.readAll();
        HashSet<String> columns = new HashSet<>();
        String temp = "";
        for(String[] splitted : colNames) {
            if (splitted[0].equals(strTableName)) {
                columns.add(splitted[1]);
                if (splitted[3].equals("true")){
                    temp = splitted[1];
                }
            }
        }
        csvReader.close();
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

    public void resetOctrees(String strTableName) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return;
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                Octree tree = readFromOctree(strTableName, tmp[0]);
                tree.reset();
                writeToOctree(tree, strTableName, tmp[0]);
            }
        }
    }

    public Vector<RecordReference> removeInAllOctrees(String strTableName, Hashtable<String, Object> htblcolNameValue) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return new Vector<>();
        Vector<RecordReference> references = new Vector<>();
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                String[] tmp2 = tmp[0].split("-");
                if(!htblcolNameValue.containsKey(tmp2[0]) || !htblcolNameValue.containsKey(tmp2[1])
                        || !htblcolNameValue.containsKey(tmp2[2]))continue;
                Octree tree = readFromOctree(strTableName, tmp[0]);
                Comparable x = (Comparable) htblcolNameValue.get(tmp2[0]);
                Comparable y = (Comparable) htblcolNameValue.get(tmp2[1]);
                Comparable z = (Comparable) htblcolNameValue.get(tmp2[2]);
                references = tree.searchReferences(x, y, z);
                break;
                //tree.remove(htblcolNameValue.get(tmp2[0]), htblcolNameValue.get(tmp2[1]), htblcolNameValue());
                //writeToOctree(tree, strTableName, tmp[0]);
            }
        }
        return references;
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {
            for(String key: htblColNameValue.keySet()){
                Object value = htblColNameValue.get(key);
                if(value instanceof String){
                    value = ((String) value).toLowerCase();
                    htblColNameValue.put(key, value);
                }
            }
            String clusteringKeyName = getClusteringKeyName(strTableName, htblColNameValue);
            int pagesCount = getTableSize(strTableName);
            Vector<Integer> toBeDeleted = new Vector<>();
            HashSet<Integer> found = new HashSet<>();

            if(htblColNameValue.isEmpty()){
                int size = getTableSize(strTableName);
                for(int i = 1; i<=size; i++) {
                    toBeDeleted.add(i);
                }
                resetOctrees(strTableName);
            }
            else if(htblColNameValue.containsKey(clusteringKeyName)) {


                Vector<RecordReference> temp = removeInAllOctrees(strTableName, htblColNameValue);
                if(temp.isEmpty()) {
                    int[] boundaries = pagesBinarySearch(pagesCount, (Comparable) htblColNameValue.get(clusteringKeyName), strTableName);
                    if (boundaries[0] != boundaries[1]) {
                        return;
                    }
                    Page currentPage = readFromPage(strTableName, boundaries[0]);
                    int idx = recordBinarySearch(currentPage.pageData, (Comparable) htblColNameValue.get(clusteringKeyName));
                    if (idx == -1 || currentPage.maxRows <= idx || !currentPage.pageData.get(idx).clusteringKey.equals(htblColNameValue.get(clusteringKeyName))) {
                        return;
                    }


                    SerializablePageRecord currentRecord = currentPage.pageData.get(idx);
                    for (String key : currentRecord.recordHash.keySet()) {
                        if (htblColNameValue.containsKey(key)
                                && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                            return;
                        }
                    }

                    currentPage.pageData.remove(idx);
                    if (currentPage.pageData.isEmpty()) {
                        toBeDeleted.add(boundaries[0]);
                        found.add(boundaries[0]);
                    } else {
                        writeToPage(strTableName, currentPage, boundaries[0]);
                    }
                }
                else {
                    w:for(RecordReference r : temp) {
                        Page currentPage = readFromPage(strTableName, Integer.parseInt(r.pageName));
                        int idx = recordBinarySearch(currentPage.pageData, (Comparable) htblColNameValue.get(clusteringKeyName));
                        if (idx == -1 || currentPage.maxRows <= idx || !currentPage.pageData.get(idx).clusteringKey.equals(htblColNameValue.get(clusteringKeyName))) {
                            continue;
                        }


                        SerializablePageRecord currentRecord = currentPage.pageData.get(idx);
                        for (String key : currentRecord.recordHash.keySet()) {
                            if (htblColNameValue.containsKey(key)
                                    && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                                continue w;
                            }
                        }

                        deleteFromAllOctrees(strTableName, currentPage.pageData.get(idx));
                        currentPage.pageData.remove(idx);
                        if (currentPage.pageData.isEmpty()) {
                            toBeDeleted.add(Integer.parseInt(r.pageName));
                            found.add(Integer.parseInt(r.pageName));
                        } else {
                            writeToPage(strTableName, currentPage, Integer.parseInt(r.pageName));
                        }

                    }
                }
            }

            else {
                Vector<RecordReference> temp = removeInAllOctrees(strTableName, htblColNameValue);
                if(temp.isEmpty()) {
                    for (int i = 1; i <= pagesCount; i++) {
                        Page currentPage = readFromPage(strTableName, i);
                        for (SerializablePageRecord currentRecord : (Vector<SerializablePageRecord>) currentPage.pageData.clone()) {
                            boolean matched = true;
                            for (String key : currentRecord.recordHash.keySet()) {
                                if (htblColNameValue.containsKey(key)
                                        && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                                    matched = false;
                                }
                            }
                            if (matched) {
                                currentPage.pageData.remove(currentRecord);
                            }
                        }
                        if (currentPage.pageData.isEmpty()) {
                            toBeDeleted.add(i);
                            found.add(i);
                        } else {
                            writeToPage(strTableName, currentPage, i);
                        }
                    }
                }else{
                    w:for(RecordReference r : temp) {
                        Page currentPage = readFromPage(strTableName, Integer.parseInt(r.pageName));
                        int idx = recordBinarySearch(currentPage.pageData, (Comparable) htblColNameValue.get(clusteringKeyName));
                        if (idx == -1 || currentPage.maxRows <= idx || !currentPage.pageData.get(idx).clusteringKey.equals(htblColNameValue.get(clusteringKeyName))) {
                            continue;
                        }


                        SerializablePageRecord currentRecord = currentPage.pageData.get(idx);
                        for (String key : currentRecord.recordHash.keySet()) {
                            if (htblColNameValue.containsKey(key)
                                    && ((currentRecord.recordHash.get(key) instanceof Null) || !htblColNameValue.get(key).equals(currentRecord.recordHash.get(key)))) {
                                continue w;
                            }
                        }

                        deleteFromAllOctrees(strTableName, currentPage.pageData.get(idx));
                        currentPage.pageData.remove(idx);
                        if (currentPage.pageData.isEmpty()) {
                            toBeDeleted.add(Integer.parseInt(r.pageName));
                            found.add(Integer.parseInt(r.pageName));
                        } else {
                            writeToPage(strTableName, currentPage, Integer.parseInt(r.pageName));
                        }

                    }
                }
            }
            deleteEntirePage(strTableName, toBeDeleted);
            HashMap<String, String> hm = new HashMap<>();
            int cnt = 0;
            for(int i = 1; i<=pagesCount; i++){
                if(found.contains(i)){
                    cnt++;
                }
                else if(cnt > 0){
                    renameFile(strTableName, i, i - cnt);
                    hm.put(i + "", i - cnt + "");
                }
            }
            if(!hm.isEmpty())renameInAllOctrees(hm, strTableName);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("ClassNotFoundException was thrown.");
        } catch (IOException | CsvException e) {
            throw new DBAppException("Problem with IO happened.");
        }
    }

    public String getIndex(String strTableName, HashSet<String> htblcolName) throws IOException, ClassNotFoundException {
        File tableFolder = new File(strTableName+"_index");
        String[] fileNames = tableFolder.list();
        if(fileNames == null) return "";
        Vector<RecordReference> references = new Vector<>();
        for(String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                String[] tmp = fileName.split("\\.");
                String[] tmp2 = tmp[0].split("-");
                if(!htblcolName.contains(tmp2[0]) || !htblcolName.contains(tmp2[1]) || !htblcolName.contains(tmp2[2]))continue;
                return tmp[0];
                //tree.remove(htblcolNameValue.get(tmp2[0]), htblcolNameValue.get(tmp2[1]), htblcolNameValue());
                //writeToOctree(tree, strTableName, tmp[0]);
            }
        }
        return "";
    }

    public Vector<SerializablePageRecord> getAllOkInsidePage(String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators, int index) throws DBAppException, IOException, ClassNotFoundException {
        Page current = readFromPage(strTableName, index);
        Vector<SerializablePageRecord> res = new Vector<>();
        for (SerializablePageRecord spr : current.pageData) {
            boolean  flag2 = true;
            for (int j = 0; j<arrSQLTerms.length; j++) {
                SQLTerm sqlTerm = arrSQLTerms[j];
                boolean flag = true;
                if (!sqlTerm._strTableName.equals(strTableName) || !spr.recordHash.containsKey(sqlTerm._strColumnName))
                    throw new DBAppException("Table name/content is not correct");
                if (!sqlTerm._strOperator.equals("=")) {
                    if (sqlTerm._strOperator.equals(">")) {
                        if (((Comparable) spr.recordHash.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue) <= 0) {
                            flag = false;
                        }
                    } else if (sqlTerm._strOperator.equals(">=")) {
                        if (((Comparable) spr.recordHash.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue) < 0) {
                            flag = false;
                        }
                    } else if (sqlTerm._strOperator.equals("<")) {
                        if (((Comparable) spr.recordHash.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue) >= 0) {
                            flag = false;
                        }
                    } else if (sqlTerm._strOperator.equals("<=")) {
                        if (((Comparable) spr.recordHash.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue) > 0) {
                            flag = false;
                        }
                    } else if (sqlTerm._strOperator.equals("!=")) {
                        if (((Comparable) spr.recordHash.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue) == 0) {
                            flag = false;
                        }
                    }
                } else {
                    if (!spr.recordHash.get(sqlTerm._strColumnName).equals(sqlTerm._objValue)) {
                        flag = false;
                    }
                }
                if (j > 0) {
                    String operator = strarrOperators[j-1];
                    if (operator.equals("AND")) {
                        if (!flag) {
                            flag2 = false;
                        }
                    } else if (operator.equals("OR")) {
                        if (flag) {
                            flag2 = true;
                        }
                    } else if (operator.equals("XOR")) {
                        if (flag) {
                            flag2 = !flag2;
                        }
                    }
                }
                else
                    flag2 = flag;
            }
            if(flag2){
                res.add(spr);
            }
        }
        return res;
    }

    public Vector<SerializablePageRecord> linearScan(String strTableName , SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException {
        int tableSize = getTableSize(strTableName);
        Vector<SerializablePageRecord> resultSet = new Vector<>();
        for (int i = 1; i <= tableSize; i++)
            resultSet.addAll(getAllOkInsidePage(strTableName, arrSQLTerms, strarrOperators, i));
        return resultSet;
    }
    //TODO: return the recordHash or SerializablePageRecord itSelf?

    Comparable getMax(Comparable x, Comparable y){
        if(x.compareTo(y) > 0)
            return x;
        return y;
    }

    Comparable getMin(Comparable x, Comparable y){
        if(x.compareTo(y) < 0)
            return x;
        return y;
    }

    public Vector<SerializablePageRecord> rangeQueries(SQLTerm[] arrSQLTerms , String[] strarrOperators) throws IOException, ClassNotFoundException, DBAppException {
        HashSet<String> colNames = new HashSet<>();
        for (SQLTerm s : arrSQLTerms) colNames.add(s._strColumnName);
        String indexName = getIndex(arrSQLTerms[0]._strTableName, colNames);
        Octree tree = readFromOctree(arrSQLTerms[0]._strTableName, indexName);
        String[] treeCols = indexName.split("-");
        HashMap<String, Comparable> minVal = new HashMap<>(), maxVal = new HashMap<>(), eqVal = new HashMap<>();
//        System.out.println(tree.root.minX+ " " + tree.root.minY + " " + tree.root.minZ);
//        System.out.println(tree.root.maxX+ " " + tree.root.maxY + " " + tree.root.maxZ);
//        System.out.println(Arrays.toString(treeCols));
        minVal.put(treeCols[0], tree.root.minX);
        minVal.put(treeCols[1], tree.root.minY);
        minVal.put(treeCols[2], tree.root.minZ);
        maxVal.put(treeCols[0], tree.root.maxX);
        maxVal.put(treeCols[1], tree.root.maxY);
        maxVal.put(treeCols[2], tree.root.maxZ);
        for(SQLTerm term : arrSQLTerms){
            if(!minVal.containsKey(term._strColumnName))continue;
            if(term._strOperator.equals("=")){
                if(eqVal.containsKey(term._strColumnName))
                    return new Vector<>();
                eqVal.put(term._strColumnName, (Comparable) term._objValue);
            }
            else if(term._strOperator.equals(">")){
               // System.out.println(term._strColumnName+" "+minVal.get(term._strColumnName)+" "+term._objValue);
                minVal.put(term._strColumnName, getMax(minVal.get(term._strColumnName), (Comparable) term._objValue));
            }
            else if(term._strOperator.equals(">=")){
                minVal.put(term._strColumnName, getMax(minVal.get(term._strColumnName), (Comparable) term._objValue));
            }
            else if(term._strOperator.equals("<")){
                maxVal.put(term._strColumnName, getMin(maxVal.get(term._strColumnName), (Comparable) term._objValue));
            }
            else if(term._strOperator.equals("<=")){
                maxVal.put(term._strColumnName, getMin(maxVal.get(term._strColumnName), (Comparable) term._objValue));
            }
        }
        for(String col : minVal.keySet()){
            if(minVal.get(col).compareTo(maxVal.get(col)) > 0)
                return new Vector<>();
        }
        Vector<RecordReference> rr = tree.rangeQuery(tree.root, minVal.get(treeCols[0]), maxVal.get(treeCols[0]),
                minVal.get(treeCols[1]), maxVal.get(treeCols[1]), minVal.get(treeCols[2]), maxVal.get(treeCols[2]));
        Vector<SerializablePageRecord> res = new Vector<>();
        HashSet<Integer> hs = new HashSet<>();
        for(RecordReference r : rr)hs.add(Integer.parseInt(r.pageName));
        for(Integer i : hs)
            res.addAll(getAllOkInsidePage(arrSQLTerms[0]._strTableName, arrSQLTerms, strarrOperators, i));
        return res;
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
        try {
            boolean flag = false;
            for (String s : strarrOperators) flag |= (s.equals("OR") || s.equals("XOR"));
            for(SQLTerm term : arrSQLTerms){
                if(term._objValue instanceof String){
                    term._objValue = ((String) term._objValue).toLowerCase();
                }
                flag |= term._strOperator.equals("!=");
            }
            HashSet<String> colNames = new HashSet<>();
            SelectIterator selectIterator = null;
            for (SQLTerm s : arrSQLTerms) {
                colNames.add(s._strColumnName);
            }
            if (flag || getIndex(arrSQLTerms[0]._strTableName, colNames).isEmpty()) {
                Vector<SerializablePageRecord> resultSet = linearScan(arrSQLTerms[0]._strTableName, arrSQLTerms, strarrOperators);
                selectIterator = new SelectIterator(resultSet);
            } else {
                Vector<SerializablePageRecord> resultSet = rangeQueries(arrSQLTerms, strarrOperators);
                selectIterator = new SelectIterator(resultSet);
            }
            return selectIterator;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
//    public Iterator parseSQL( StringBuffer strbufSQL ) throws
//    DBAppException{}



    private static void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", fields[0].trim());

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));
            row.put("date_added", dateAdded);

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private static void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }

    private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }

    private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }

    private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }

    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }

    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    public static void main(String[] args) throws Exception {
        DBApp db = new DBApp();
//        db.init();
//        createCoursesTable(db);
//        createPCsTable(db);
//        createTranscriptsTable(db);
//        createStudentTable(db);
//        insertPCsRecords(db, 500);
//        insertTranscriptsRecords(db, 500);
//        insertStudentRecords(db, 500);
//        insertCoursesRecords(db, 500);
//        System.out.println("done");
//        for(int i = 1; i<=2; i++){
//            FileInputStream fis = new FileInputStream("courses/" + i + ".class");
//            ObjectInputStream ois = new ObjectInputStream(fis);
//            Page b = (Page) ois.readObject();
//            for(SerializablePageRecord s : b.pageData){
//                for(String key : s.recordHash.keySet()){
//                    System.out.print(key + " " + s.recordHash.get(key) + " ");
//                }
//                System.out.println();
//            }
//            System.out.println("=========");
//
//        }
        // incorrent insert
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("pc_id","10000");
//        htblColNameValue.put("student_id", "43-1111");
//
//        db.insertIntoTable( "pcs" , htblColNameValue );
        //2nd incorrect insert
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("pc_id",10000);
//        htblColNameValue.put("student_id", "43-1111");
//        htblColNameValue.put("ghalat", "43-1111");
//        db.insertIntoTable( "pcs" , htblColNameValue );
        //3rd incorrect insert
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("pc_id",10000);
//        db.insertIntoTable( "pcs" , htblColNameValue );
        //first update
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("first_name","Hamada");
//        htblColNameValue.put("last_name","Hamadaa");
//        htblColNameValue.put("gpa",1.2);
//        htblColNameValue.put("dob", new Date("1990/12/31"));
//
//        db.updateTable("students", "44-1562", htblColNameValue);
        //incorrect update
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("first_name","Hamada");
//        htblColNameValue.put("last_name","Hamadaa");
//        htblColNameValue.put("gpa",1.2);
//        htblColNameValue.put("dob", new Date("1990/12/31"));
//        htblColNameValue.put("ghalat", "12343");
//        db.updateTable("students", "44-1562", htblColNameValue);
        //first update
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("date_added", new Date("1932/06/30"));
//        db.deleteFromTable("courses", htblColNameValue);
        //second update
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("hours", 16);
//        db.deleteFromTable("courses", htblColNameValue);
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[2];
//        arrSQLTerms[0]= new SQLTerm();
//        arrSQLTerms[1]=new SQLTerm();
//        arrSQLTerms[0]._strTableName = "courses";
//        arrSQLTerms[0]._strColumnName= "course_name";
//        arrSQLTerms[0]._strOperator = "=";
//        arrSQLTerms[0]._objValue = "FjGZmL";
//        arrSQLTerms[1]._strTableName = "courses";
//        arrSQLTerms[1]._strColumnName= "hours";
//        arrSQLTerms[1]._strOperator = "<=";
//        arrSQLTerms[1]._objValue =24;
//        String[]strarrOperators = new String[1];
//        strarrOperators[0] = "OR";
//        long start = System.currentTimeMillis();
//        Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);
//
//        while(it.hasNext()) {
//            System.out.println(it.next());
//        }
//        long end = System.currentTimeMillis();
//
//        System.out.println("time difference is "+(end-start));

//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        arrSQLTerms[0]= new SQLTerm();
//        arrSQLTerms[1]=new SQLTerm();
//        arrSQLTerms[2]=new SQLTerm();
//        arrSQLTerms[0]._strTableName = "courses";
//        arrSQLTerms[0]._strColumnName= "course_id";
//        arrSQLTerms[0]._strOperator = "=";
//        arrSQLTerms[0]._objValue = "0950";
//        arrSQLTerms[1]._strTableName = "courses";
//        arrSQLTerms[1]._strColumnName= "hours";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue =20;
//        arrSQLTerms[2]._strTableName = "courses";
//        arrSQLTerms[2]._strColumnName= "date_added";
//        arrSQLTerms[2]._strOperator = "=";
//        arrSQLTerms[2]._objValue = new Date("2008/06/31");
//        String[]strarrOperators = new String[2];
//        strarrOperators[0] = "AND";
//        strarrOperators[1] = "AND";
//        long start = System.currentTimeMillis();
//
//
//
//        Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);
//
//        while(it.hasNext()) {
//            System.out.println(it.next());
//        }
//        long end = System.currentTimeMillis();
//
//        System.out.println("time difference is "+(end-start));
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        arrSQLTerms[0]= new SQLTerm();
//        arrSQLTerms[1]=new SQLTerm();
//        arrSQLTerms[2]=new SQLTerm();
//        arrSQLTerms[0]._strTableName = "courses";
//        arrSQLTerms[0]._strColumnName= "course_id";
//        arrSQLTerms[0]._strOperator = "=";
//        arrSQLTerms[0]._objValue = "0950";
//        arrSQLTerms[1]._strTableName = "courses";
//        arrSQLTerms[1]._strColumnName= "hours";
//        arrSQLTerms[1]._strOperator = "<=";
//        arrSQLTerms[1]._objValue =20;
//        arrSQLTerms[2]._strTableName = "courses";
//        arrSQLTerms[2]._strColumnName= "course_name";
//        arrSQLTerms[2]._strOperator = "=";
//        arrSQLTerms[2]._objValue = "swusvz";
//        String[]strarrOperators = new String[2];
//        strarrOperators[0] = "OR";
//        strarrOperators[1] = "AND";
//        long start = System.currentTimeMillis();
//
//
//
//        Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);
//
//        while(it.hasNext()) {
//            System.out.println(it.next());
//        }
//        long end = System.currentTimeMillis();
//
//        System.out.println("time difference is "+(end-start));
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[2];
//        arrSQLTerms[0]= new SQLTerm();
//        arrSQLTerms[1]=new SQLTerm();
//        arrSQLTerms[0]._strTableName = "courses";
//        arrSQLTerms[0]._strColumnName= "course_id";
//        arrSQLTerms[0]._strOperator = "=";
//        arrSQLTerms[0]._objValue = "0950";
//        arrSQLTerms[1]._strTableName = "courses";
//        arrSQLTerms[1]._strColumnName= "hours";
//        arrSQLTerms[1]._strOperator = "!=";
//        arrSQLTerms[1]._objValue =16;
//        String[]strarrOperators = new String[1];
//        strarrOperators[0] = "OR";
//        long start = System.currentTimeMillis();
//
//
//
//        Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);
//
//        while(it.hasNext()) {
//            System.out.println(it.next());
//        }
//        long end = System.currentTimeMillis();
//
//        System.out.println("time difference is "+(end-start));
//        db.createIndex("courses", new String[] {"course_id","hours","date_added"});
        //Octree o = db.readFromOctree("courses", "course_id-hours-date_added");
        //o.print();

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[3];
        arrSQLTerms[0]= new SQLTerm();
        arrSQLTerms[1]=new SQLTerm();
        arrSQLTerms[2]=new SQLTerm();
        arrSQLTerms[0]._strTableName = "courses";
        arrSQLTerms[0]._strColumnName= "course_id";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "0950";
        arrSQLTerms[1]._strTableName = "courses";
        arrSQLTerms[1]._strColumnName= "hours";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue =20;
        arrSQLTerms[2]._strTableName = "courses";
        arrSQLTerms[2]._strColumnName= "date_added";
        arrSQLTerms[2]._strOperator = "=";
        arrSQLTerms[2]._objValue = new Date("2008/06/31");
        String[]strarrOperators = new String[2];
        strarrOperators[0] = "AND";
        strarrOperators[1] = "AND";

        db.selectFromTable(arrSQLTerms, strarrOperators);     //<-- this is the first call we need to avoid reading from disk

        long start = System.currentTimeMillis();


        Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);

        while(it.hasNext()) {
            System.out.println(it.next());
        }
        long end = System.currentTimeMillis();

        System.out.println("time difference is "+(end-start));
    }

}
