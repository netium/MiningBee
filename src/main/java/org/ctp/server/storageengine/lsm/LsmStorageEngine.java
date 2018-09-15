package org.ctp.server.storageengine.lsm;

import org.ctp.server.configuration.ServerConfiguration;
import org.ctp.server.storageengine.AbstractStorageEngine;
import org.ctp.server.storageengine.command.*;
import org.ctp.server.storageengine.lsm.sstable.NewSSTable;
import org.ctp.server.storageengine.lsm.sstable.SSTableCreator;
import org.ctp.server.storageengine.lsm.sstable.SSTableSequenceReader;
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

import static org.ctp.server.storageengine.lsm.DBFilenameUtil.DBFILE_EXTENSION;

public class LsmStorageEngine extends AbstractStorageEngine {
    final Logger logger = LoggerFactory.getLogger(LsmStorageEngine.class);

    private static final int ACQUIRE_LOCK_TIMEOUT = 20;
    private static final TimeUnit ACQUIRE_LOCK_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private static final int MEMTABLE_THRESHOLD = 1 * 1024;

    private LinkedBlockingQueue<Command> commandQueryQueue = new LinkedBlockingQueue<Command>();

    private Thread commandQueryThread = null;
    private Thread compactAndMergeThread = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    private final ReadWriteLock inMemIndexListLock = new ReentrantReadWriteLock();
    private final Lock inMemIndexListUpdateLock = inMemIndexListLock.writeLock();
    private final Lock inMemIndexListReadLock = inMemIndexListLock.readLock();

    private CopyOnWriteArrayList<NewSSTable> sstables = new CopyOnWriteArrayList<>();

    private String dbFileFolder;

    private AtomicReference<Memtable> currentMemtableARef;
    private AtomicReference<Memtable> flushingMemtableARef = new AtomicReference<>();

    private WriteAheadLog writeAheadLog = null;

    public LsmStorageEngine(ServerConfiguration serverConfiguration) {
        super(serverConfiguration);
        currentMemtableARef = new AtomicReference<>();
        currentMemtableARef.set(new Memtable());

        File dbFolder = new File(serverConfiguration.getDbPath());
        if (!dbFolder.exists()) {
            dbFolder.mkdirs();
        }
        if (!dbFolder.isDirectory()) {
            throw new LsmStorageEngineException("The path " + dbFileFolder + " is not a directory");
        }

        this.dbFileFolder = serverConfiguration.getDbPath();
    }

    @Override
    public void start() {
        try {
            buildSegmentInMemIndexList(new File(this.dbFileFolder));

            compactAndMergeThread = new Thread(new SSTableCompactor());
            compactAndMergeThread.start();

            initWriteAheadLog();

            replayWriteAheadLog();

            commandQueryThread = new Thread(() -> processCommands());
            commandQueryThread.start();
        } catch (Exception e) {
            throw new LsmStorageEngineException("The LSM storage engine cannot start.", e);
        }
    }

    @Override
    public void put(String key, String value, ResultHandler resultHandler) {
        PutCommand command = new PutCommand(key, value, resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void read(String key, ResultHandler resultHandler) {
        GetCommand command = new GetCommand(key, resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void delete(String key, ResultHandler resultHandler) {
        DeleteCommand command = new DeleteCommand(key, resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void compareAndSet(String key, String oldValue, String newValue, ResultHandler resultHandler) {
        CompareAndSetCommand command = new CompareAndSetCommand(key, oldValue, newValue, resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void getDiagnosisInfo(ResultHandler resultHandler) {
        GetEngineInfoCommand command = new GetEngineInfoCommand(resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void flush(ResultHandler resultHandler) {
        FlushCommand command = new FlushCommand(resultHandler);
        if (!commandQueryQueue.offer(command)) {
            resultHandler.handle(
                    new CommandResult(ResultStatus.OVERLOAD, null, null)
            );
        }
    }

    @Override
    public void close() throws IOException {
        createSSTable();
    }

    private void createSSTable() {
        Memtable currentMemtable = currentMemtableARef.get();
        if (currentMemtable.size() == 0)
            return;

        while (!this.flushingMemtableARef.compareAndSet(null, currentMemtable)) ;

        this.currentMemtableARef.set(new Memtable());

        final Path dbPath = generateSSTablePath();

        executorService.submit(() -> flushMemTable(dbPath));
    }

    private Path generateSSTablePath() {
        return Paths.get(dbFileFolder, DBFilenameUtil.generateNewSSTableDBName());
    }

    private void removeSSTables(ArrayList<NewSSTable> oldIndexList) throws Exception {
        for (NewSSTable index : oldIndexList) {
            index.close();
            logger.info("Delete sstable: {}" , index.getFile());
            index.getFile().delete();
        }
    }

    private void mergeSSTables(ArrayList<SSTableSequenceReader> readers, SSTableCreator ssTableWriter) throws IOException {
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
                            ssTableWriter.write(pair);
                        }
                        minKeyWritten = true;
                    } else {
                        readers.get(i).read();
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

    private void buildSegmentInMemIndexList(File folder) throws IOException {
        File[] dbFiles = readSegmentFileInDesOrder(folder);
        for (File dbFile : dbFiles) {
            sstables.add(new NewSSTable(dbFile));
        }
    }

    private File[] readSegmentFileInDesOrder(File folder) {
        File[] dbFiles = folder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(DBFILE_EXTENSION));

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

        try {
            logger.info("Star to flushing memtable to " + sstableFilePath.toString());
            Memtable flushingMemtable = flushingMemtableARef.get();
            try (SSTableCreator ssTableCreator = new SSTableCreator(sstableFilePath.toFile(), flushingMemtable.size())) {
                for (Pair<String, String> entry : flushingMemtable) {
                    ssTableCreator.write(entry);
                }
            }
            final NewSSTable ssTable = new NewSSTable(sstableFilePath.toFile());

            writeAheadLog = writeAheadLog.createNewLogFile();

            runTimeoutCheckedCriticalSection(inMemIndexListUpdateLock,
                    () -> sstables.add(0, ssTable));

            if (!flushingMemtableARef.compareAndSet(flushingMemtable, null)) {
                final String errorString = "The flushing table is changed during the flushing, critical error occur!";
                logger.error(errorString);
                throw new LsmStorageEngineException(errorString);
            }
        } catch (IOException e) {
            throw new LsmStorageEngineException(e);
        } catch (InterruptedException e) {
            logger.warn(marker, "The lock acquiring is interrupted");
        }
        logger.info("Finish flushing memtable to " + sstableFilePath.toString());
    }

    private void compactAndMerge() throws Exception {
        if (!isMergeAndCompactNeed()) {
            logger.info("The merge-n-compact criteria is not reached, skip");
            return;
        }

        logger.info("Start to do the merge-n-compact");
        Path dbPath = createNewDBMergedFile();
        Path dbTempPath = Paths.get(dbPath.toString() + ".tmp");

        ArrayList<SSTableSequenceReader> readers = new ArrayList<>();

        ArrayList<NewSSTable> sstableListSnapshot = null;

        long totalItems = 0;
        while (!inMemIndexListReadLock.tryLock(ACQUIRE_LOCK_TIMEOUT, ACQUIRE_LOCK_TIMEOUT_UNIT))
            logger.warn("failed to acquire read lock in 10sec");

        try {
            sstableListSnapshot = new ArrayList<>(sstables);
            for (NewSSTable sstable : sstables) {
                readers.add(new SSTableSequenceReader(sstable.iterator()));
                totalItems += sstable.getNumOfItems();
            }
        } finally {
            inMemIndexListReadLock.unlock();
        }
        SSTableCreator creator = new SSTableCreator(dbTempPath.toFile(), (int)totalItems);

        try {
            mergeSSTables(readers, creator);
        }
        finally {
            creator.close();
        }

        dbTempPath.toFile().renameTo(dbPath.toFile());

        NewSSTable ssTable = new NewSSTable(dbPath.toFile());

        while (!inMemIndexListUpdateLock.tryLock(10, TimeUnit.SECONDS))
            logger.warn("failed to acquire write lock in 10sec");

        try {
            ArrayList<NewSSTable> oldIndexList = sstableListSnapshot;
            removeSSTables(oldIndexList);

            for (NewSSTable inMemIndex : sstableListSnapshot) {
                sstables.remove(inMemIndex);
            }
            sstables.add(ssTable);
        } finally {
            inMemIndexListUpdateLock.unlock();
        }

        logger.info("Finish the merge-n-compact");
    }

    private Path createNewDBMergedFile() {
        Stream<String> filenames = sstables.stream().map(p -> p.getFile().getAbsolutePath());
        Stream<File> files = filenames.map(p -> new File(p));
        String newMergedDBName = DBFilenameUtil.generateNewMergedDBName(files);
        return Paths.get(dbFileFolder, newMergedDBName);
    }

    private boolean isMergeAndCompactNeed() throws InterruptedException {
        while (!inMemIndexListReadLock.tryLock(10, TimeUnit.SECONDS))
            logger.warn("failed to acquire read lock in 10sec for merging/compacting criteria checking");

        try {
            logger.debug("Current NewSSTable files: " + sstables.size());
            return sstables.size() >= 3;
        } finally {
            inMemIndexListReadLock.unlock();
        }
    }

    private void runTimeoutCheckedCriticalSection(Lock lock, Runnable criticalSection) throws InterruptedException {
        if (!lock.tryLock(ACQUIRE_LOCK_TIMEOUT, ACQUIRE_LOCK_TIMEOUT_UNIT))
            throw new AcquireLockTimeoutException("Failed to acquire lock " + lock.toString() + " in thread: " + Thread.currentThread().getName());

        try {
            criticalSection.run();
        } finally {
            lock.unlock();
        }
    }

    private void processCommands() {
        logger.info("Start to listen to command queue and processing the command");
        while (true) {
            try {
                Command command = commandQueryQueue.take();
                if (command instanceof GetCommand) {
                    processQueryCommand((GetCommand) command);
                } else if (command instanceof DeleteCommand) {
                    processDeleteCommand((DeleteCommand) command);
                } else if (command instanceof PutCommand) {
                    processPutCommand((PutCommand) command);
                } else if (command instanceof CompareAndSetCommand) {
                    processCompareAndSetCommand((CompareAndSetCommand) command);
                } else if (command instanceof FlushCommand) {
                    processFlushCommand((FlushCommand) command);
                } else if (command instanceof GetEngineInfoCommand) {
                    processGetEngineInfoCommand((GetEngineInfoCommand) command);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void processQueryCommand(final GetCommand command) {
        String key = command.getKey();
        try {
            Memtable currentMemtable = this.currentMemtableARef.get();
            Memtable flushingMemtable = this.flushingMemtableARef.get();

            if (currentMemtable.containsKey(key)) {
                command.getResultHandler().handle(
                        new CommandResult(ResultStatus.OK, currentMemtable.get(key), null)
                );
            } else if (flushingMemtable != null && flushingMemtable.containsKey(key)) {
                command.getResultHandler().handle(
                        new CommandResult(ResultStatus.OK, flushingMemtable.get(key), null)
                );
            } else {
                for (NewSSTable sstable : sstables) {
                    Pair<String, String> keyValuePair = sstable.get(key);
                    if (keyValuePair != null) {
                        command.getResultHandler().handle(
                                new CommandResult(keyValuePair != null ? ResultStatus.OK : ResultStatus.FAILED, keyValuePair.getValue(), null)
                        );
                        return;
                    }
                }
            }
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.FAILED, null, null)
            );
        } catch (Throwable e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.ERROR, null, e)
            );
        }
    }

    private void processDeleteCommand(final DeleteCommand command) {
        final String key = command.getKey();
        try {
            Memtable currentMemtable = currentMemtableARef.get();
            writeAheadLog.appendItem(key, null);
            currentMemtable.delete(key);
            if (currentMemtable.getRawDataSize() >= MEMTABLE_THRESHOLD) {
                createSSTable();
            }
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.OK, null, null)
            );
        } catch (Throwable e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.ERROR, null, e)
            );
        }
    }

    private void processPutCommand(final PutCommand command) {
        final String key = command.getKey();
        final String value = command.getValue();
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

            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.OK, value, null)
            );
        } catch (Throwable e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.ERROR, null, e)
            );
        }
    }

    private void processCompareAndSetCommand(final CompareAndSetCommand command) {
        command.getResultHandler().handle(
                new CommandResult(ResultStatus.NO_SUPPORTED, null, null)
        );
    }

    private void processFlushCommand(final FlushCommand command) {
        try {
            createSSTable();
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.OK, null, null)
            );
        } catch (Throwable e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.ERROR, null, e)
            );
        }
    }

    private void processGetEngineInfoCommand(GetEngineInfoCommand command) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Engine class: " + getClass().getName() + "\n");
            sb.append("Database file folder: " + dbFileFolder + "\n");
            sb.append("In memory index tables: " + "\n");
            for (NewSSTable sstable : sstables) {
                sb.append("\t SSTable: " + sstable.getFile().getAbsolutePath() + "\n");
            }
            if (sstables.size() == 0) {
                sb.append("\n");
            }

            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.OK, sb.toString(), null)
            );
        } catch (Throwable e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
            command.getResultHandler().handle(
                    new CommandResult(ResultStatus.ERROR, null, e)
            );
        }
    }

    private class SSTableCompactor implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(SSTableCompactor.class);
        private final Marker marker = MarkerFactory.getMarker("MERGE-N-COMPACT");

        @Override
        public void run() {
            logger.info(marker, "Compact and merge background thread starting...");
            while (true) {
                try {
                    compactAndMerge();
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    logger.info(marker, "The merge and compact thread is interrupted and is stopping.");
                    return;
                }
                catch (Exception e) {
                    logger.error(marker, "Compact and merge met exception: {}\n, Stacktrace: {}", e.toString(), e.getStackTrace().toString());
                }
            }
        }

    }

}


