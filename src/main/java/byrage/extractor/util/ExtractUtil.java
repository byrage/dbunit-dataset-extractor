package byrage.extractor.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;
import java.net.URL;

public class ExtractUtil {

    public static URL readResource(final String fileName) throws IOException {

        return Resources.getResource(fileName);
    }

    public static String readResourceAsString(final String fileName) throws IOException {

        return Resources.toString(Resources.getResource(fileName), Charsets.UTF_8);
    }
}
