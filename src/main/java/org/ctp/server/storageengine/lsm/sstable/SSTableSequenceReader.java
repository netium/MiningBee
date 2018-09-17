package org.ctp.server.storageengine.lsm.sstable;

import org.ctp.util.Pair;

import java.io.EOFException;
import java.util.Iterator;

public class SSTableSequenceReader {
    private final Iterator<Pair<String, String>> iterator;

    private Pair<String, String> cacheItem;

    public SSTableSequenceReader(Iterator<Pair<String, String>> sstableIterator) {
        iterator = sstableIterator;
    }

    public boolean isEof() {
        return cacheItem == null && !iterator.hasNext();
    }

    public String peekNextKey() throws EOFException {
        chargeCacheItemIfNeeded();

        return cacheItem.getKey();
    }

    public Pair<String, String> read() throws EOFException {
        chargeCacheItemIfNeeded();
        assert cacheItem != null;
        Pair<String, String> item = cacheItem;
        cacheItem = null;
        return item;
    }

    private void chargeCacheItemIfNeeded() throws EOFException {
        if (cacheItem != null)
            return;

        if (iterator.hasNext()) {
            cacheItem = iterator.next();
        } else {
            throw new EOFException();
        }
    }
}
