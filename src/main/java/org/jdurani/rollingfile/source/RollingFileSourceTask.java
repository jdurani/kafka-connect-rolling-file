package org.jdurani.rollingfile.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;
import org.jdurani.rollingfile.exception.ReadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source task.
 */
public class RollingFileSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileSourceTask.class);
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("^([0-9]{10})-[0-9]{19}\\.txt$");
    private String dir;
    private int batchSize;
    private boolean ignoreTimestamp;
    private boolean ignorePartition;
    private List<RollingFileReader> toRead;
    private final Set<String> fullyReadFiles = new HashSet<>();

    @Override
    public String version() {
        return VersionHolder.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        dir = props.get(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG);
        batchSize = Integer.parseInt(props.get(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG));
        ignorePartition = Boolean.parseBoolean(props.get(RollingFileConfig.ROLLING_FILE_IGNORE_PARTITION_CONFIG));
        ignoreTimestamp = Boolean.parseBoolean(props.get(RollingFileConfig.ROLLING_FILE_IGNORE_TIMESTAMP_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() {
        try {
            if (toRead == null || toRead.isEmpty()) {
                toRead = nextFilesToRead();
            }
            List<SourceRecord> recs = new ArrayList<>(batchSize);
            for (Iterator<RollingFileReader> iterator = toRead.iterator(); iterator.hasNext(); ) {
                RollingFileReader r = iterator.next();
                SourceRecord next;
                while ((next = r.nextRecord(ignoreTimestamp)) != null) {
                    recs.add(next);
                    if (recs.size() >= batchSize) {
                        return recs;
                    }
                }
                iterator.remove();
                fullyReadFiles.add(r.getFileAbsolutePath());
                r.close();
            }
            return recs;
        } catch (IOException e) {
            throw new ReadException("Error getting data", e);
        }
    }

    /**
     * @return all files currently available for reading
     *
     * @throws IOException in case of error
     */
    List<RollingFileReader> nextFilesToRead() throws IOException {
        File base = new File(dir);
        if (!base.exists()) {
            return Collections.emptyList();
        }
        File[] topics = base.listFiles(File::isDirectory);
        if (topics == null) {
            return Collections.emptyList();
        }
        List<File> out = new ArrayList<>();
        for (File topic : topics) {
            File[] files = topic.listFiles(x -> x.isFile()
                    && !fullyReadFiles.contains(x.getAbsolutePath()) // do not load already read files
                    && FILE_NAME_PATTERN.matcher(x.getName()).matches());
            if (files != null) {
                for (File r : files) {
                    out.add(r.getAbsoluteFile());
                }
            }
        }
        out.sort(Comparator.<File, String>comparing(x -> x.getParentFile().getName()).thenComparing(File::getName));
        OffsetStorageReader osr = context.offsetStorageReader();
        List<RollingFileReader> list = new ArrayList<>();
        for (File x : out) {
            list.add(new RollingFileReader(x, x.getParentFile().getName(), ignorePartition ? null : getPartition(x), osr));
        }
        return list;
    }

    /**
     * Reads kafka partition from file name.
     *
     * @param f file
     *
     * @return partition
     */
    private int getPartition(File f) {
        Matcher matcher = FILE_NAME_PATTERN.matcher(f.getName());
        matcher.matches();
        return Integer.parseInt(matcher.group(1));
    }

    @Override
    public void stop() {
        if (toRead != null) {
            for (RollingFileReader r : toRead) {
                try {
                    r.close();
                } catch (IOException e) {
                    LOG.error("Error closing reader.", e);
                }
            }
        }
    }
}
