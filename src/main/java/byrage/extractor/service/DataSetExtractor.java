package byrage.extractor.service;

import byrage.extractor.model.Config;
import byrage.extractor.model.DbType;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DataSetExtractor {

    private static final Logger log = LoggerFactory.getLogger(DataSetExtractor.class);

    private static final String CONFIG_PROPERTIES = "config.properties";
    private static final String DATA_SET_EXTENSION = ".xml";
    private static final String TABLES_AND_QUERIES_SEPARATOR = "-";

    public boolean extract() {

        try {
            Config config = loadConfig();
            log.info("output file={}", config.getOutputFileName() + DATA_SET_EXTENSION);

            QueryDataSet dataSet = createDataSet(config);
            FlatXmlDataSet.write(dataSet, new FileOutputStream(config.getOutputFileName() + DATA_SET_EXTENSION));

            return true;
        } catch (Exception e) {
            log.error("extracting data set is failed.", e);
            return false;
        }
    }

    Config loadConfig() throws IOException {

        try {
            InputStream inputStream = new FileInputStream(CONFIG_PROPERTIES);
            Properties prop = new Properties();
            prop.load(inputStream);

            return new Config(prop);
        } catch (IOException e) {
            throw new IOException("loadConfig is failed.", e);
        }

    }

    QueryDataSet createDataSet(Config config) throws Exception {

        try {
            DbType dbType = config.getType();
            Class.forName(dbType.getDriverName());
            String url = dbType.getJdbcPrefix() + config.getHost() + ":" + config.getPort() + "/" + config.getDbName();

            Connection connection = DriverManager.getConnection(url, config.getId(), config.getPassword());
            IDatabaseConnection dbConnection = new DatabaseConnection(connection);
            DatabaseConfig dbConfig = dbConnection.getConfig();
            dbConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, dbType.getDataTypeFactoryClass().newInstance());

            QueryDataSet dataSet = new QueryDataSet(dbConnection);

            for (String tableAndQuery : config.getTablesAndQueries()) {
                if (StringUtils.contains(tableAndQuery, TABLES_AND_QUERIES_SEPARATOR)) {
                    String[] s = StringUtils.split(tableAndQuery, TABLES_AND_QUERIES_SEPARATOR);
                    dataSet.addTable(s[0].trim(), s[1].trim());
                } else {
                    dataSet.addTable(tableAndQuery.trim());
                }
            }

            return dataSet;
        } catch (Exception e) {
            throw new Exception("createDataSet failed.", e);
        }
    }

}
