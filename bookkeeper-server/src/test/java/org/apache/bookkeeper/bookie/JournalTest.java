package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.bookkeeper.bookie.ApacheBookieJournalUtil.*;
import static org.apache.bookkeeper.bookie.JournalUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JournalTest {


    public static long writtenBytes;
    private Journal journal;
    private long journalID;
    private long totalBytesToRead;

    @Before
    public void before() throws Exception {
        this.journal = createJournal();
        this.journalID = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        //ora writtenBytes ha il numero di byte scritti sul Journal, bisogna aggiungere quelli relativi all'update
        //della versione del journal stesso (che è un int).
        writtenBytes += Integer.BYTES;




    }

    @After
    public void after() {
        clearDirs();
        shutDownBookie();

    }
    @Test
    public void testScanJournal() throws Exception {

        long readBytes = this.journal.scanJournal(this.journalID, 0, new DummyJournalScan());
        assertEquals(writtenBytes, readBytes);
    }


    //Metodo Apache. Usiamo uno scanner dummy poichè è uno Unit test, e ci "svincoliamo" dalla correttezza e dall'interazione
    //delle altre componenti
    private class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
            //non fa nulla, metodo dummy
        }
    }

}
