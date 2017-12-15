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
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static byrage.extractor.util.ExtractUtil.readResource;

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
            Config config = loadConfig(CONFIG_PROPERTIES);

            outputFile = config.getOutputFileName() + DATA_SET_EXTENSION;
            log.info("output file name={}", outputFile);

            IDatabaseConnection connection = getConnection(config);
            IDataSet dataSet = createDataSet(connection, config.getRowName(), config.getQuery());

            FlatXmlDataSet.write(dataSet, new FileOutputStream(outputFile));
            return true;
        } catch (Exception e) {
            cleanFile(outputFile);
            log.error("extracting data set is failed.", e.getMessage());
            return false;
        } finally {
            try { dbConnection.close(); } catch (SQLException e) {}
            try { connection.close(); } catch (SQLException e) {}
        }
    }

    @VisibleForTesting
    Config loadConfig(String propertyFileName) throws IOException, URISyntaxException {
        try {

            InputStream is = new FileInputStream(new File(readResource(propertyFileName).getFile()));
            Properties prop = new Properties();
            prop.load(is);

            Config config = new Config(prop);
            log.debug("query={}", config.getQuery());
            if (StringUtils.isBlank(config.getOutputFileName())) {
                throw new IllegalArgumentException("output file name is invalid.");
            }

            return config;
        } catch (IOException e) {
            throw new IOException("loadConfig is failed.", e);
        }
    }

    @VisibleForTesting
    Config loadConfig(InputStream inputStream) throws IOException, URISyntaxException {
        try {

            Properties prop = new Properties();
            prop.load(inputStream);

            Config config = new Config(prop);
            if (StringUtils.isBlank(config.getOutputFileName())) {
                throw new IllegalArgumentException("output file name is blank.");
            }
            return config;
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

    private QueryDataSet createDataSet(IDatabaseConnection connection, String rowName, String query) throws AmbiguousTableNameException {

        try {
            QueryDataSet dataSet = new QueryDataSet(connection);

            if (StringUtils.isEmpty(rowName)) {
                dataSet.addTable(query.trim());
            } else {
                dataSet.addTable(rowName.trim(), query.trim());
            }
            return dataSet;
        } catch (AmbiguousTableNameException e) {
            throw new AmbiguousTableNameException("createDataSet is failed. tableName is ambiguous", e);
        }
    }

    private void cleanFile(String outputFile) {

        File file = new File(outputFile);
        if (file.exists()) {
            file.delete();
        }
    }
}
