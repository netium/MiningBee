/*
 * Copyright (c) 2018.
 *
 * Author: Netium (Bo Zhou)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.netium.server.storageengine.lsm;

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
