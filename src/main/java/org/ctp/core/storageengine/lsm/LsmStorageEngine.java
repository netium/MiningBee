package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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
    public String getDiagnosisInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("Engine class: " + getClass().getName() + "\n");
        sb.append("Database file folder: " + dbFileFolder + "\n");
        sb.append("In memory index tables: " + "\n");
        for (InMemIndex inMemIndex : segmentInMemIndexList) {
            sb.append("\t SSTable: " + inMemIndex.getSSTable().getFilename() + "\n");
        }

        return sb.toString();
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
        Path dbPath = generateSSTablePath();
        try (SSTable.SSTableWriter ssTableWriter = SSTable.openForWriteItem(dbPath.toString())) {
            ArrayList<SSTable.SSTableSequenceReader> readers = new ArrayList<>();
            for (InMemIndex inMemIndex : segmentInMemIndexList) {
                readers.add(inMemIndex.getSSTable().getSequenceReader());
            }
            mergeSSTables(readers, ssTableWriter);
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }

        SSTable ssTable = new SSTable(dbPath.toString());
        InMemIndex memIndex = new InMemIndex(ssTable);

        ArrayList<InMemIndex> oldIndexList = new ArrayList<>(segmentInMemIndexList);
        removeSSTables(oldIndexList);

        segmentInMemIndexList.clear();
        segmentInMemIndexList.add(memIndex);
    }

    private void removeSSTables(ArrayList<InMemIndex> oldIndexList) {
        for (InMemIndex index : oldIndexList) {
            File file = new File(index.getSSTable().getFilename());
            file.delete();
        }
    }

    private void mergeSSTables(ArrayList<SSTable.SSTableSequenceReader> readers, SSTable.SSTableWriter ssTableWriter) throws IOException {
        for (int i = readers.size() - 1; i >= 0; i--) {
            if (readers.get(i).isEof())
                readers.remove(i);
        }

        while (readers.size() != 0) {
            String minKey = readers.get(0).peekNextKey();

            for (int i = 1; i < readers.size(); i++) {
                if (readers.get(i).peekNextKey().compareTo(minKey) < 0)
                    minKey = readers.get(i).peekNextKey();
            }
            System.out.println("minKey: " + minKey);

            boolean minKeyWritten = false;
            for (int i = 0; i < readers.size(); i++) {
                if (readers.get(i).peekNextKey().compareTo(minKey) == 0) {
                    if (!minKeyWritten) {
                        Pair<String, String> pair = readers.get(i).read();
                        ssTableWriter.append(pair.getKey(), pair.getValue());
                        minKeyWritten = true;
                        System.out.println(String.format("Write <%s, %s>", pair.getKey(), pair.getValue()));
                    }
                    else {
                        readers.get(i).skipItem();
                    }
                }
            }

            for (int i = readers.size() - 1; i >= 0; i--) {
                if (readers.get(i).isEof())
                    readers.remove(i);
            }
        }
    }

    private SSTable.SSTableSequenceReader getSSTableSequenceReader(SSTable ssTable) {
        try {
            return ssTable.getSequenceReader();
        }
        catch (Exception e) {
            return null;
        }
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
                long integerName1 = Long.parseLong(filename1.substring(0, filename1.indexOf('.')));
                long integerName2 = Long.parseLong(filename2.substring(0, filename2.indexOf('.')));
                if (integerName1 < integerName2)
                    return 1;
                else if (integerName1 == integerName2)
                    return 0;
                return -1;
        });

        return dbFiles;
    }
}
