package main.java;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {

    Vector<SerializablePageRecord> pageData;
    int maxRows;

    public Page() throws IOException {
        pageData = new Vector<>();
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        FileInputStream fileInputStream = new FileInputStream(fileName);
        prop.load(fileInputStream);
        maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountingTablePage"));
    }

    public void addRecord(SerializablePageRecord data) {
        pageData.add(data);
    }

}
