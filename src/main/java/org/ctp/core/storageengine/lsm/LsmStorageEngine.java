package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LsmStorageEngine implements IStorageEngine {
    private final int MEMTABLE_THRESHOLD = 1 * 1024;
    private ArrayList<InMemIndex> segmentInMemIndexList = new ArrayList();

    private String dbFileFolder;

    private volatile Memtable currentMemtable = new Memtable();
    private volatile Memtable flushingMemtable = null;

    public LsmStorageEngine() {
    }

    @Override
    public void initEngine(String dbFileFolder) {
        File dbFolder = new File(dbFileFolder);
        if (!dbFolder.exists()) {
            dbFolder.mkdirs();
        }
        if (!dbFolder.isDirectory()) {
            throw new LsmStorageEngineException("The path " + dbFileFolder + " is not a directory");
        }

        this.dbFileFolder = dbFileFolder;

        buildSegmentInMemIndexList(dbFolder);
    }

    @Override
    public boolean put(String key, String value) {
        try {
            if (key == null || key.length() == 0) {
                throw new IllegalArgumentException();
            }
            if (value == null || value.length() == 0) {
                throw new IllegalArgumentException();
            }

            currentMemtable.put(key, value);
            if (currentMemtable.getRawSize() >= MEMTABLE_THRESHOLD) {
                createSSTable();
            }

            return true;
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String read(String key) {
        try {
            if (currentMemtable.containsKey(key))
                return currentMemtable.get(key);
            else if (flushingMemtable != null && flushingMemtable.containsKey(key))
                return flushingMemtable.get(key);
            else {
                for (InMemIndex indexCache : segmentInMemIndexList) {
                    if (indexCache.contains(key)) {
                        return indexCache.get(key);
                    }
                }
            }
            return null;
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            currentMemtable.delete(key);
            if (currentMemtable.getRawSize() >= MEMTABLE_THRESHOLD) {
                createSSTable();
            }
            return true;
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean compareAndSet(String key, String oldValue, String newValue) {
        if (key == null)
            throw new IllegalArgumentException();
        if (oldValue == null)
            throw new IllegalArgumentException();
        if (newValue == null)
            throw new IllegalArgumentException();

        String value = read(key);
        if (!oldValue.equals(value))
            return false;
        put(key, newValue);
        return true;
    }

    @Override
    public void close() throws IOException {
        createSSTable();
    }

    public boolean flush() {
        try {
            createSSTable();
            return true;
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return false;
        }
    }

    private void createSSTable() {
        this.flushingMemtable = this.currentMemtable;
        this.currentMemtable = new Memtable();

        Path dbPath = generateSSTablePath();
        SSTable ssTable = null;

        try {
            ssTable = SSTable.flush(this.flushingMemtable, dbPath.toString());
        } catch (IOException e) {
            throw new LsmStorageEngineException(e);
        }

        InMemIndex memIndex = new InMemIndex(ssTable);
        segmentInMemIndexList.add(0, memIndex);
        compactAndMerge();
    }

    private Path generateSSTablePath() {
        return Paths.get(dbFileFolder, generateSSTableName());
    }

    private String generateSSTableName() {
        return System.currentTimeMillis() + ".db";
    }

    private void compactAndMerge() {

    }

    private void buildSegmentInMemIndexList(File folder) {
        File[] dbFiles = readSegmentFileInDesOrder(folder);
        for (File dbFile : dbFiles) {
            segmentInMemIndexList.add(new InMemIndex(new SSTable(dbFile.getAbsolutePath())));
        }
    }

    private File[] readSegmentFileInDesOrder(File folder) {
        File[] dbFiles = folder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && pathname.getName().endsWith(".db");
            }
        });

        Arrays.sort(dbFiles, (File file1, File file2) -> {
                String filename1 = file1.getName();
                String filename2 = file2.getName();
                int integerName1 = Integer.parseInt(filename1.substring(0, filename1.indexOf('.')));
                int integerName2 = Integer.parseInt(filename2.substring(0, filename2.indexOf('.')));
                return integerName1 - integerName2;
        });

        return dbFiles;
    }
}
