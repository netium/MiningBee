package org.ctp.core.storageengine.lsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class WriteAheadLog {
    private final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

    private final static String FILE_SUFFIX = ".wal";

    private final File logFolder;
    private final File logFile;

    public WriteAheadLog(String folder) throws IOException {
        logFolder = new File(folder);
        if (logFolder.exists() && logFolder.isFile()) {
            throw new IllegalArgumentException();
        }
        if (!logFolder.exists()) {
            logFolder.mkdirs();
        }

        correctLogFolder();
        logFile = getLogFile();
    }

    private void correctLogFolder() {
        File[] logFiles = logFolder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(FILE_SUFFIX));
        if (logFiles.length > 1) {
            Arrays.sort(logFiles, (file1, file2) -> {
                if (file1.lastModified() > file2.lastModified()) {
                    return -1;
                }
                else if (file1.lastModified() == file2.lastModified()) {
                    return 0;
                }
                else {
                    return 1;
                }
            });
            for (int i = 1; i < logFiles.length; i++) {
                logFiles[i].delete();
            }
        }
    }

    private File getLogFile() throws IOException {
        File[] logFiles = logFolder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(FILE_SUFFIX));
        if (logFiles.length > 1) {
            logger.error("The write ahead log folder contains {} (>0) log files", logFiles.length);
        }
        if (logFiles.length > 0) {
            return logFiles[0];
        }
        else {
            File logFile = File.createTempFile("", FILE_SUFFIX, logFolder);
            return logFile;
        }
    }


}
