package byrage.extractor.service;

import byrage.extractor.model.Config;
import byrage.extractor.model.DbType;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static junit.framework.TestCase.fail;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

// TODO : integration test with H2
public class DataSetExtractorTest {

    private FileSystem fs;

    private DataSetExtractor dataSetExtractor;

    @Before
    public void setUp() throws Exception {

        dataSetExtractor = new DataSetExtractor();
        fs = Jimfs.newFileSystem(Configuration.unix());
    }

    @After
    public void tearDown() throws Exception {

        fs.close();
    }

    @Test
    public void loadConfig() throws Exception {

        // given
        final String type = "mysql";
        final String host = "localhost";
        final String port = "3306";
        final String dbName = "test";
        final String id = "root";
        final String password = "root";
        final String rowName = "user";
        final String outputFileName = "output";

        final String fileName = "test.properties";
        final String fileContent = buildPropertiesContent(outputFileName, type, host, port, dbName, id, password, rowName);

        Path path = createTestFile(fileName, fileContent);

        // when
        Config config = dataSetExtractor.loadConfig(Files.newInputStream(path));

        // then
        assertThat(config.getOutputFileName(), is(outputFileName));
        assertThat(config.getType(), is(DbType.MySQL));
        assertThat(config.getHost(), is(host));
        assertThat(config.getPort(), is(port));
        assertThat(config.getDbName(), is(dbName));
        assertThat(config.getId(), is(id));
        assertThat(config.getPassword(), is(password));
        assertThat(config.getRowName(), is(rowName));
    }

    @Test
    public void loadConfig_propertyTypeMySQLIgnoreCase() throws Exception {

        // given
        String[] types = {"mysql", "MYSQL", "MySQL"};
        for (String type : types) {

            final String fileName = "test.properties";
            final String fileContent = buildPropertiesContent(type);

            Path path = createTestFile(fileName, fileContent);

            // when
            Config config = dataSetExtractor.loadConfig(Files.newInputStream(path));

            // then
            assertThat(config.getType(), is(DbType.MySQL));
        }
    }

    @Test
    public void loadConfig_propertyTypeOracleIgnoreCase() throws Exception {

        // given
        String[] types = {"oracle", "ORACLE", "Oracle"};
        for (String type : types) {

            final String fileName = "config.properties";
            final String fileContent = buildPropertiesContent(type);

            Path path = createTestFile(fileName, fileContent);

            // when
            Config config = dataSetExtractor.loadConfig(Files.newInputStream(path));

            // then
            assertThat(config.getType(), is(DbType.Oracle));
        }
    }

    @Test
    public void loadConfig_propertyType_MSSQL_IgnoreCase() throws Exception {

        // given
        String[] types = {"mssql", "MSSQL", "MSSql"};
        for (String type : types) {

            final String fileName = "test.properties";
            final String fileContent = buildPropertiesContent(type);

            Path path = createTestFile(fileName, fileContent);

            // when
            Config config = dataSetExtractor.loadConfig(Files.newInputStream(path));

            // then
            assertThat(config.getType(), is(DbType.MsSql));
        }
    }

    @Test
    public void loadConfig_propertyTypeInvalid() throws Exception {

        // given
        String[] types = {"null", null, "", "MariaDB", "H2"};
        for (String type : types) {

            final String fileName = "test.properties";
            final String fileContent = buildPropertiesContent(type);

            Path path = createTestFile(fileName, fileContent);

            // when
            try {
                dataSetExtractor.loadConfig(Files.newInputStream(path));
                fail("exception is not occurred. types="+type);
            } catch (Exception e) {
                // then
                assertThat(e instanceof IllegalArgumentException, is(true));
                assertThat(e.getMessage(), is("doesn't support db type. type=" + type));
            }
        }
    }

    @Test(expected = IOException.class)
    public void loadConfig_occurIOException() throws Exception {

        // given
        InputStream inputStream = new InputStream() {

            @Override
            public int read() throws IOException {

                throw new IOException("just test");
            }
        };

        // when
        Config config = dataSetExtractor.loadConfig(inputStream);

    }

    private Path createTestFile(String name, String content) throws IOException {

        Path path = fs.getPath(".", name);
        BufferedWriter bufferedWriter = Files.newBufferedWriter(path, Charset.forName("UTF-8"));
        bufferedWriter.write(content);
        bufferedWriter.close();
        return path;
    }

    private String buildPropertiesContent(String outputFileName, String type, String host, String port, String dbName,
                                          String id, String password, String rowName) {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("OUTPUT_FILE_NAME=").append(outputFileName).append("\n");
        stringBuilder.append("TYPE=").append(type).append("\n");
        stringBuilder.append("HOST=").append(host).append("\n");
        stringBuilder.append("PORT=").append(port).append("\n");
        stringBuilder.append("DB_NAME=").append(dbName).append("\n");
        stringBuilder.append("ID=").append(id).append("\n");
        stringBuilder.append("PASSWORD=").append(password).append("\n");
        stringBuilder.append("ROW_NAME=").append(rowName).append("\n");

        return stringBuilder.toString();
    }

    private String buildPropertiesContent(String type) {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("OUTPUT_FILE_NAME=test").append("\n");
        stringBuilder.append("TYPE=").append(type).append("\n");

        return stringBuilder.toString();
    }
}