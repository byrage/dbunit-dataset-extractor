package byrage;

import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DataSetExtractor {

    private static final String CONFIG_PROPERTIES = "config.properties";
    private static final String DATASET_EXTENSION = ".xml";
    private static final String TABLES_AND_QUERIES_SEPARATOR = "-";

    // TODO : handle exceptions
    public static void main(String[] args) throws Exception {

        InputStream inputStream = new FileInputStream(CONFIG_PROPERTIES);

        Properties prop = new Properties();
        prop.load(inputStream);

        final String type = prop.getProperty("TYPE").trim();
        final String host = prop.getProperty("HOST").trim();
        final String port = prop.getProperty("PORT").trim();
        final String dbName = prop.getProperty("DB_NAME").trim();
        final String id = prop.getProperty("ID").trim();
        final String password = prop.getProperty("PASSWORD").trim();
        final String tablesAndQueries = prop.getProperty("TABLES_AND_QUERIES").trim();
        final String outputFileName = prop.getProperty("OUTPUT_FILE_NAME").trim();

        DbType dbType = getDbTypeByString(type);
        String[] tableAndQueryArray = StringUtils.split(tablesAndQueries, "/");

        Class.forName(dbType.getDriverName());

        String url = dbType.getJdbcPrefix() + host + ":" + port + "/" + dbName;
        Connection jdbcConnection = DriverManager.getConnection(url, id, password);
        IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);

        QueryDataSet partialDataSet = new QueryDataSet(connection);

        for (String tableAndQuery : tableAndQueryArray) {
            if (StringUtils.contains(tableAndQuery, TABLES_AND_QUERIES_SEPARATOR)) {
                String[] s = StringUtils.split(tableAndQuery, TABLES_AND_QUERIES_SEPARATOR);
                partialDataSet.addTable(s[0].trim(), s[1].trim());
            } else {
                partialDataSet.addTable(tableAndQuery.trim());
            }
        }

        FlatXmlDataSet.write(partialDataSet, new FileOutputStream(outputFileName + DATASET_EXTENSION));
        System.out.println("writing XmlDataSet complete. result=" + outputFileName + DATASET_EXTENSION);
    }

    private static DbType getDbTypeByString(String type) throws Exception {

        for (DbType dbType : DbType.values()) {
            if (StringUtils.equalsIgnoreCase(dbType.name(), type)) {
                return dbType;
            }
        }

        throw new Exception("doesn't support db type. type=" + type);
    }
}
