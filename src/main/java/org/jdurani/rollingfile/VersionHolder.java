package org.jdurani.rollingfile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector version.
 */
public class VersionHolder {

    private static final Logger LOG = LoggerFactory.getLogger(VersionHolder.class);
    private static final String DEFAULT_VERSION = "0.0.0";
    public static final String VERSION = loadVersion();

    /**
     * @return loaded version from resources of default version
     */
    private static String loadVersion() {
        try (InputStream is = VersionHolder.class.getResourceAsStream("/version.txt");
                ByteArrayOutputStream bos = new ByteArrayOutputStream(100)) {
            if (is == null) {
                throw new NullPointerException("No 'version.txt' file.");
            }
            byte[] buff = new byte[100];
            int i;
            while ((i = is.read(buff)) >= 0) {
                bos.write(buff, 0, i);
            }
            return bos.toString();
        } catch (Exception e) {
            LOG.error("Error loading version.", e);
            return DEFAULT_VERSION;
        }
    }
}
