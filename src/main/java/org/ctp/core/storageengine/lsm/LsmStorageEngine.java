package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class LsmStorageEngine implements IStorageEngine {
    final Logger logger = LoggerFactory.getLogger(LsmStorageEngine.class);

    private final int MEMTABLE_THRESHOLD = 1 * 1024;

    private Thread compactAndMergeThread = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    private final ReadWriteLock inMemIndexListLock = new ReentrantReadWriteLock();
    private final Lock inMemIndexListUpdateLock = inMemIndexListLock.writeLock();
    private final Lock inMemIndexListReadLock = inMemIndexListLock.readLock();

    private CopyOnWriteArrayList<InMemIndex> segmentInMemIndexList = new CopyOnWriteArrayList<>();

    private String dbFileFolder;

    private AtomicReference<Memtable> currentMemtableARef;
    private AtomicReference<Memtable> flushingMemtableARef = new AtomicReference<>();

    private WriteAheadLog writeAheadLog = null;

    public LsmStorageEngine() {
        currentMemtableARef = new AtomicReference<>();
        currentMemtableARef.set(new Memtable());
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

        compactAndMergeThread = new Thread(new SSTableCompactor());
        compactAndMergeThread.start();

        initWriteAheadLog();

        replayWriteAheadLog();
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

            Memtable currentMemtable = this.currentMemtableARef.get();

            writeAheadLog.appendItem(key, value);
            currentMemtable.put(key, value);
            if (currentMemtable.getRawDataSize() >= MEMTABLE_THRESHOLD) {
                createSSTable();
            }

            return true;
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String read(String key) {
        try {
            Memtable currentMemtable = this.currentMemtableARef.get();
            Memtable flushingMemtable = this.flushingMemtableARef.get();

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
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            Memtable currentMemtable = currentMemtableARef.get();
            writeAheadLog.appendItem(key, null);
            currentMemtable.delete(key);
            if (currentMemtable.getRawDataSize() >= MEMTABLE_THRESHOLD) {
                createSSTable();
            }
            return true;
        } catch (Exception e) {
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
        if (segmentInMemIndexList.size() == 0) {
            sb.append("\n");
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
        } catch (Exception e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            return false;
        }
    }

    private void createSSTable() {
        Memtable currentMemtable = currentMemtableARef.get();
        if (currentMemtable.size() == 0)
            return;

        while (!this.flushingMemtableARef.compareAndSet(null, currentMemtable));

        this.currentMemtableARef.set(new Memtable());

        final Path dbPath = generateSSTablePath();

        Future future = executorService.submit(() -> flushMemTable(dbPath));
    }

    private Path generateSSTablePath() {
        return Paths.get(dbFileFolder, DBFilenameUtil.generateNewSSTableDBName());
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
            logger.debug("minKey: " + minKey);

            boolean minKeyWritten = false;
            for (int i = 0; i < readers.size(); i++) {
                if (readers.get(i).peekNextKey().compareTo(minKey) == 0) {
                    if (!minKeyWritten) {
                        Pair<String, String> pair = readers.get(i).read();
                        if (pair.getValue() != null) {
                            logger.debug("Write <{}, {}>", pair.getKey(), pair.getValue());
                            ssTableWriter.append(pair.getKey(), pair.getValue());
                        }
                        minKeyWritten = true;
                    } else {
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
        } catch (Exception e) {
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
        File[] dbFiles = folder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(".db"));

        Arrays.sort(dbFiles, new DBFileComparor());

        return dbFiles;
    }

    private void initWriteAheadLog() {
        writeAheadLog = new WriteAheadLog("./wal");
    }

    private void replayWriteAheadLog() {
        logger.debug("Start to replay WAL");
        writeAheadLog.prepareForReplay();
        Memtable currentMemtable = currentMemtableARef.get();
        while (!writeAheadLog.isEof()) {
            Pair<String, String> pair = writeAheadLog.replayNextItem();
            logger.debug("Replay item: {}, {}", pair.getKey(), pair.getValue());
            currentMemtable.put(pair.getKey(), pair.getValue());
        }
        logger.debug("Replay WAL done");
    }

    private void flushMemTable(final Path sstableFilePath) {
        Marker marker = MarkerFactory.getMarker("FLUSHMEMTABLE");

        SSTable ssTable = null;

        try {
            logger.info("Star to flushing memtable to " + sstableFilePath.toString());
            Memtable flushingMemtable = flushingMemtableARef.get();
            ssTable = SSTable.flush(flushingMemtable, sstableFilePath.toString());
            writeAheadLog = writeAheadLog.createNewLogFile();
            if (!flushingMemtableARef.compareAndSet(flushingMemtable, null)) {
                final String errorString = "The flushing table is changed during the flushing, critical error occur!";
                logger.error(errorString);
                throw new LsmStorageEngineException(errorString);
            }
        } catch (IOException e) {
            throw new LsmStorageEngineException(e);
        }

        InMemIndex memIndex = new InMemIndex(ssTable);

        try {
            runTimeoutCheckedCriticalSection(inMemIndexListUpdateLock,
                    () -> segmentInMemIndexList.add(0, memIndex));
            logger.info("Finish flushing memtable to " + sstableFilePath.toString());
        }
        catch (InterruptedException e) {
            logger.warn(marker, "The lock acquiring is interrupted");
        }
    }

    private void runTimeoutCheckedCriticalSection(Lock lock, Runnable criticalSection) throws InterruptedException {
        if (!lock.tryLock(20, TimeUnit.SECONDS))
            throw new AcquireLockTimeoutException("Failed to acquire lock " + lock.toString() + " in thread: " + Thread.currentThread().getName());

        try {
            criticalSection.run();
        }
        finally {
            lock.unlock();
        }
    }

    private class SSTableCompactor implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(SSTableCompactor.class);
        private final Marker marker = MarkerFactory.getMarker("MERGE-N-COMPACT");

        @Override
        public void run() {
            logger.info("Compact and merge background thread starting...");
            while (true) {
                try {
                    compactAndMerge();
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    logger.warn(marker, "The merge and compact thread is interrupted.");
                }
            }
            // logger.info("Compact and merge background thread stopping...");
        }

        private void compactAndMerge() throws InterruptedException {
            if (!isMergeAndCompactNeed()) {
                logger.info(marker, "The merge-n-compact criteria is not reached, skip");
                return;
            }

            logger.info(marker, "Start to do the merge-n-compact");
            Path dbPath = createNewDBMergedFile();
            Path dbTempPath = Paths.get(dbPath.toString() + ".tmp");

            ArrayList<InMemIndex> inMemIndexListSnapshot = null;
            try (SSTable.SSTableWriter ssTableWriter = SSTable.openForWriteItem(dbPath.toString())) {
                ArrayList<SSTable.SSTableSequenceReader> readers = new ArrayList<>();

                while (!inMemIndexListReadLock.tryLock(10, TimeUnit.SECONDS))
                    logger.warn(marker, "failed to acquire read lock in 10sec");

                try {
                    inMemIndexListSnapshot = new ArrayList<>(segmentInMemIndexList);
                    for (InMemIndex inMemIndex : segmentInMemIndexList) {
                        readers.add(inMemIndex.getSSTable().getSequenceReader());
                    }
                }
                finally {
                    inMemIndexListReadLock.unlock();
                }

                mergeSSTables(readers, ssTableWriter);
            } catch (Exception e) {
                logger.error(e.toString());
                logger.error(e.getStackTrace().toString());
            }

            dbTempPath.toFile().renameTo(dbPath.toFile());

            SSTable ssTable = new SSTable(dbPath.toString());
            InMemIndex memIndex = new InMemIndex(ssTable);

            while (!inMemIndexListUpdateLock.tryLock(10, TimeUnit.SECONDS))
                logger.warn(marker, "failed to acquire write lock in 10sec");

            try {
                ArrayList<InMemIndex> oldIndexList = inMemIndexListSnapshot;
                removeSSTables(oldIndexList);

                for (InMemIndex inMemIndex : inMemIndexListSnapshot) {
                    segmentInMemIndexList.remove(inMemIndex);
                }
                segmentInMemIndexList.add(memIndex);
            }
            finally {
                inMemIndexListUpdateLock.unlock();
            }

            logger.info(marker, "Finish the merge-n-compact");
        }

        private Path createNewDBMergedFile() {
            Stream<String> filenames = segmentInMemIndexList.stream().map(p->p.getSSTable().getFilename());
            Stream<File> files = filenames.map(p-> new File(p));
            String newMergedDBName = DBFilenameUtil.generateNewMergedDBName(files);
            return Paths.get(dbFileFolder, newMergedDBName);
        }

        private boolean isMergeAndCompactNeed() throws InterruptedException {
            while (!inMemIndexListReadLock.tryLock(10, TimeUnit.SECONDS))
                logger.warn(marker, "failed to acquire read lock in 10sec for merging/compacting criteria checking");

            try {
                logger.debug(marker, "Current SSTable files: " + segmentInMemIndexList.size());
                return segmentInMemIndexList.size() >= 3;
            }
            finally {
                inMemIndexListReadLock.unlock();
            }
        }
    }

}


