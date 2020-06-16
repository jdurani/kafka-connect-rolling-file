package org.jdurani.rollingfile;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;

public class Utils {

    private static int idx = 0;

    public static Path tmpDir(boolean deleteIfExists) throws IOException {
        StackTraceElement ste = new Exception().getStackTrace()[1];
        String cls = ste.getClassName();
        Path path = FileSystems.getDefault().getPath("target",
                "test_data_tmp",
                cls.substring(cls.lastIndexOf('.') + 1),
                String.valueOf(++idx));
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        } else if (deleteIfExists) {
            delete(path.toFile());
        }
        return path;
    }

    public static void delete(File f) {
        if (f.isDirectory()) {
            for (File fs : f.listFiles()) {
                delete(fs);
            }
        }
        Assertions.assertTrue(f.delete(), f.getAbsolutePath() + " not deleted");
    }
}
