package org.ctp.core.storageengine.lsm;

import java.io.File;
import java.util.Comparator;

public class DBFileComparor implements Comparator<File> {

    @Override
    public int compare(File file1, File file2) {
        String filename1 = file1.getName();
        String filename2 = file2.getName();
        long integerName1 = DBFilenameUtil.getFileOrderIndex(filename1);
        long integerName2 = DBFilenameUtil.getFileOrderIndex(filename2);
        if (integerName1 < integerName2) {
            return 1;
        }
        else if (integerName1 == integerName2) {
            return filename1.compareTo(filename2);
        }
        return -1;
    }
}
