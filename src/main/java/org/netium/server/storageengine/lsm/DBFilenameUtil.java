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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public final class DBFilenameUtil {
    private DBFilenameUtil() {}

    public static final String DBFILE_EXTENSION = ".db";

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
        return System.currentTimeMillis() + DBFILE_EXTENSION;
    }

    public static String generateNewMergedDBName(Stream<File> files) {
        File latestFile = getLatestFilename(files);
        String latestFileName = latestFile.getName();
        String nameWithoutExtension = latestFileName.substring(0, latestFileName.lastIndexOf('.'));
        nameWithoutExtension = nameWithoutExtension + "-m";
        String newMergedDBName = nameWithoutExtension + DBFILE_EXTENSION;
        return newMergedDBName;
    }
}
