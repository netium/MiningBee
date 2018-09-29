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

package org.ctp.server.storageengine.lsm;

import org.ctp.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;

public class WriteAheadLog {
    private final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

    private final static String FILE_SUFFIX = ".wal";

    private final File logFolder;
    private final File logFile;
    private RandomAccessFile logAccessFile;

    public WriteAheadLog(String folder) {
        this(new File(folder));
    }

    public WriteAheadLog(File logFolder) {
        try {
            if (logFolder.exists() && logFolder.isFile()) {
                throw new IllegalArgumentException();
            }
            this.logFolder = logFolder;
            if (!logFolder.exists()) {
                logFolder.mkdirs();
            }

            correctLogFolder();
            logFile = getLogFile();
            logAccessFile = new RandomAccessFile(logFile, "rw");
        }
        catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
    }

    public synchronized void prepareForReplay() {
        try {
            logAccessFile.seek(0);
        }
        catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
    }

    public synchronized boolean isEof() {
        try {
            return logAccessFile.getFilePointer() == logAccessFile.length();
        } catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
    }

    public synchronized Pair<String, String> replayNextItem() {
        return replayNextItem(logAccessFile);
    }

    public synchronized void appendItem(String key, String value) {
        logger.debug("WAL: APPEND: {}, {}", key, value);
        appendItem(logAccessFile, key, value);
    }

    public synchronized WriteAheadLog createNewLogFile() {
        try {
            logAccessFile.close();
            logFile.delete();
            return new WriteAheadLog(logFolder);
        } catch (IOException e) {
            throw new WriteAheadLogException(e);
        }

    }


    private Pair<String, String> replayNextItem(DataInput dataInput) {
        try {
            int keyLength = dataInput.readByte();
            if (keyLength < 0) {
                throw new IllegalArgumentException();
            }
            byte[] keyBuf = new byte[keyLength];
            dataInput.readFully(keyBuf);
            String key = new String(keyBuf);

            int valueLength = dataInput.readInt();
            if (valueLength < 0) {
                throw new IllegalArgumentException();
            }
            String value = null;
            if (valueLength > 0) {
                byte[] valueBuf = new byte[valueLength];
                dataInput.readFully(valueBuf);
                value = new String(valueBuf);
            }

            return new Pair<>(key, value);
        }
        catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
    }

    private void appendItem(RandomAccessFile dataOutput, String key, String value) {
        try {
            logAccessFile.seek(logAccessFile.length());
            if (key == null || key.length() == 0) {
                throw new IllegalArgumentException("The key is null or empty");
            }
            if (value != null && value.length() == 0) {
                throw new IllegalArgumentException("The value is empty");
            }

            byte[] keyBytes = key.getBytes(Charset.defaultCharset());
            if (keyBytes.length > 127) {
                throw new IllegalArgumentException("The key exceed the length");
            }

            byte[] valueBytes = value == null ? new byte[0] : value.getBytes(Charset.defaultCharset());
            if (valueBytes.length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The value exceed the length");
            }

            dataOutput.writeByte(keyBytes.length);
            dataOutput.write(keyBytes);
            dataOutput.writeInt(valueBytes.length);
            if (valueBytes.length > 0) {
                dataOutput.write(valueBytes);
            }
            logAccessFile.getFD().sync();
        }
        catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
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

    private File getLogFile() {
        try {
            File[] logFiles = logFolder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(FILE_SUFFIX));
            if (logFiles.length > 1) {
                logger.error("The write ahead log folder contains {} (>0) log files", logFiles.length);
            }
            if (logFiles.length > 0) {
                return logFiles[0];
            } else {
                File logFile = File.createTempFile("wal", FILE_SUFFIX, logFolder);
                return logFile;
            }
        }
        catch (IOException e) {
            throw new WriteAheadLogException(e);
        }
    }
}
