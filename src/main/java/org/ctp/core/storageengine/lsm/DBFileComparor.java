package org.ctp.core.storageengine.lsm;

import java.io.File;
import java.util.Comparator;

public class DBFileComparor implements Comparator<File> {

    @Override
    public int compare(File file1, File file2) {
        String filename1 = file1.getName();
        String filename2 = file2.getName();
        long integerName1 = Long.parseLong(filename1.substring(0, filename1.indexOf('.')));
        long integerName2 = Long.parseLong(filename2.substring(0, filename2.indexOf('.')));
        if (integerName1 < integerName2)
            return 1;
        else if (integerName1 == integerName2)
            return 0;
        return -1;
    }
}
