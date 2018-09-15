package org.ctp.server.storageengine.lsm.sstable;

import org.ctp.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class SSTableReadWriteTest {
    private final static String TEST_DATA_DIR = "./testdata/";

    @Before
    public void cleanupSSTableFile() {
        File dir = new File(TEST_DATA_DIR );
        if (!dir.exists())
            dir.mkdirs();

        File[] files = dir.listFiles(p->true);

        for (File file : files) {
            file.delete();
        }
    }

    @Test
    public void testReadAfterWrite() throws IOException {
        File file = new File(TEST_DATA_DIR + "test1.sstable");
        SSTableCreator creator = new SSTableCreator(file, 500);
        creator.write(
                new Pair("a", "testa")
        );
        creator.write(
                new Pair("b", "testb")
        );

        NewSSTable table = creator.flush();
        Assert.assertEquals(table.get("b").getKey(), "b");
        Assert.assertEquals(table.get("b").getValue(), "testb");

        Assert.assertEquals(table.get("a").getKey(), "a");
        Assert.assertEquals(table.get("a").getValue(), "testa");

        Assert.assertEquals(table.get("c"), null);
        Assert.assertEquals(table.getNumOfItems(), 2);

        Iterator<Pair<String, String>> sstableIterator = table.iterator();
        Assert.assertTrue(sstableIterator.hasNext());
        Pair<String, String> keyValuePair = sstableIterator.next();
        Assert.assertEquals(keyValuePair.getKey(), "a");
        Assert.assertEquals(keyValuePair.getValue(), "testa");

        Assert.assertTrue(sstableIterator.hasNext());
        keyValuePair = sstableIterator.next();
        Assert.assertEquals(keyValuePair.getKey(), "b");
        Assert.assertEquals(keyValuePair.getValue(), "testb");

        Assert.assertFalse(sstableIterator.hasNext());
    }
}
