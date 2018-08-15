package org.ctp.core.storageengine.lsm;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public final class DBFilenameUtil {
    private DBFilenameUtil() {}

    public static long getFileOrderIndex(String filename) {
        String filenameWithoutExtension = filename.substring(0, filename.lastIndexOf('.'));
        String orderIndex = filenameWithoutExtension.split("-")[0];
        return Long.parseLong(orderIndex);
    }

    public static File getLatestFilename(File[] filenames) {
        return getLatestFilename(Arrays.stream(filenames));
    }

    public static File getLatestFilename(List<File> filenames) {
        return getLatestFilename(filenames.stream());
    }

    public static File getLatestFilename(Stream<File> filenames) {
        return filenames.max(new DBFileComparor()).get();
    }

    public static String generateNewSSTableDBName() {
        return System.currentTimeMillis() + ".db";
    }

    public static String generateNewMergedDBName(Stream<File> files) {
        File latestFile = getLatestFilename(files);
        String latestFileName = latestFile.getName();
        String nameWithoutExtension = latestFileName.substring(0, latestFileName.lastIndexOf('.'));
        nameWithoutExtension = nameWithoutExtension + "-m";
        String newMergedDBName = nameWithoutExtension + ".db";
        return newMergedDBName;
    }
}
