package byrage.extractor.service;

import byrage.extractor.model.Config;
import byrage.extractor.model.DbType;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.*;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DataSetExtractor {

    private static final Logger log = LoggerFactory.getLogger(DataSetExtractor.class);

    private static final String CONFIG_PROPERTIES = "config.properties";
    private static final String DATA_SET_EXTENSION = ".xml";
    private static final String QUERIES_SEPARATOR = "-";
    private static final String ALL_TABLES = "*";

    private Connection connection;
    private IDatabaseConnection dbConnection;

    public boolean run() {

        String outputFile = "";
        try {
            InputStream fileInputStream = new FileInputStream(CONFIG_PROPERTIES);
            Config config = loadConfig(fileInputStream);

            if (StringUtils.isBlank(config.getOutputFileName())) {
                throw new IllegalArgumentException("output file name is invalid.");
            } else {
                outputFile = config.getOutputFileName() + DATA_SET_EXTENSION;
                log.info("output file={}", outputFile);
            }

            IDatabaseConnection connection = getConnection(config);
            IDataSet dataSet;

            if (config.getQueries().length == 1 && StringUtils.equals(config.getQueries()[0], ALL_TABLES)) {
                dataSet = createAllDataSet(connection);
            } else {
                dataSet = createDataSet(connection, config.getQueries());
            }

            FlatXmlDataSet.write(dataSet, new FileOutputStream(outputFile));
            return true;
        } catch (Exception e) {
            cleanFile(outputFile);
            log.error("extracting data set is failed.", e);
            return false;
        } finally {
            try { dbConnection.close(); } catch (SQLException e) {}
            try { connection.close(); } catch (SQLException e) {}
        }
    }

    @VisibleForTesting
    Config loadConfig(InputStream fileInputStream) throws IOException {

        try {
            Properties prop = new Properties();
            prop.load(fileInputStream);

            return new Config(prop);
        } catch (IOException e) {
            throw new IOException("loadConfig is failed.", e);
        }
    }

    private IDatabaseConnection getConnection(Config config) throws Exception {

        try {
            DbType dbType = config.getType();
            Class.forName(dbType.getDriverName());
            connection = DriverManager.getConnection(config.getUrl(), config.getId(), config.getPassword());
            dbConnection = new DatabaseConnection(connection);

            DatabaseConfig dbConfig = dbConnection.getConfig();
            dbConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, dbType.getDataTypeFactoryClass().newInstance());
            return dbConnection;
        } catch (Exception e) {
            throw new Exception("getConnection is failed.", e);
        }
    }

    private QueryDataSet createDataSet(IDatabaseConnection connection, String[] queries) throws AmbiguousTableNameException {

        try {
            QueryDataSet dataSet = new QueryDataSet(connection);

            for (String query : queries) {
                if (StringUtils.contains(query, QUERIES_SEPARATOR)) {
                    String[] s = StringUtils.split(query, QUERIES_SEPARATOR);
                    dataSet.addTable(s[0].trim(), s[1].trim());
                } else {
                    dataSet.addTable(query.trim());
                }
            }

            return dataSet;
        } catch (AmbiguousTableNameException e) {
            throw new AmbiguousTableNameException("createDataSet is failed. tableName is ambiguous", e);
        }
    }

    private IDataSet createAllDataSet(IDatabaseConnection connection) throws SQLException {

        return connection.createDataSet();
    }

    private void cleanFile(String outputFile) {

        File file = new File(outputFile);
        if (file.exists()) {
            file.delete();
        }
    }
}
