package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.bookkeeper.bookie.JournalUtil.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(Parameterized.class)
public class JournalTest {


    public static long WRITTEN_BYTES;
    private final long EMPTY_JOURNAL_BYTES = 516L;
    private Journal journal;
    private final int journalToUse;
    private final long journalPos;
    private final Journal.JournalScanner scanner;
    private final Expected expected;
    private Exception actualException = null;
    private long journalID;
    private static volatile boolean wasProcessCalled = false;


    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][]{
             //journalToScan         //journalPos        //Scanner           //Expected
                {0,                      -1,          new DummyJournalScan(),     Expected.EXACT_BYTES},
                {0,                       0,          new DummyJournalScan(),     Expected.EXACT_BYTES},
                {0,                       1,          new DummyJournalScan(),     Expected.PASSED},
                {1,                      -1,          new DummyJournalScan(),     Expected.PASSED},
                {1,                       0,          new DummyJournalScan(),     Expected.PASSED},
                {1,                       1,          new DummyJournalScan(),     Expected.PASSED},
                {0,                       0,          null,                       Expected.NPE},
                {0,                       0,          new InvalidJournalScan(),   Expected.IOE},
                {0,             Integer.MAX_VALUE,    new InvalidJournalScan(),   Expected.NO_READ},

        });
    }

    public JournalTest(int journalID, long journalPos, Journal.JournalScanner scanner, Expected expected) {
        this.journalToUse = journalID;
        this.journalPos = journalPos;
        this.scanner = scanner;
        this.expected = expected;


    }

    @Before
    public void before() throws Exception {
        wasProcessCalled = false; //reset
        this.journal = createJournal();
        this.journalID = this.journalToUse == 0 ?  Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0) : 1;
        //ora writtenBytes ha il numero di byte scritti sul Journal, bisogna aggiungere quelli relativi all'update
        //della versione del journal stesso (che è un int).
        WRITTEN_BYTES += Integer.BYTES;





    }

    @After
    public void after() {
        clearDirs();
        shutDownBookie();

    }
    @Test
    public void testScanJournal() {

        long readBytes = 0;
        try {
            readBytes = this.journal.scanJournal(this.journalID, this.journalPos, this.scanner);
        } catch (IOException | NullPointerException e) {
            actualException = e;
        }
        if (this.expected == Expected.EXACT_BYTES) {
            //ID = 1, pos <= 0
            assertNull(actualException);
            assertEquals(WRITTEN_BYTES, readBytes);
        }
        else if (this.expected == Expected.PASSED) {
            assertNull(actualException);
            //Non ho letto dal journal che ho scritto in createJournal(), ma da uno vuoto creato sul momento dello scan
            assertNotEquals(WRITTEN_BYTES, readBytes);
            if (this.journalToUse == 1)
                //Questo controllo va fatto solo se l'id del journal era di un journal non esistente,
                //perchè in quel caso ne viene creato uno vuoto, di dimensione nota.
                assertEquals(EMPTY_JOURNAL_BYTES, readBytes); //Non ho letto il primo journal, ne leggo uno che è vuoto
        }
        else {
            if (this.expected == Expected.NPE)
                assertEquals(this.actualException.getClass(), NullPointerException.class);
            else if (this.expected == Expected.IOE)
                assertEquals(this.actualException.getClass(), IOException.class);
            else if (this.expected == Expected.NO_READ)
                assertFalse(wasProcessCalled);
        }

    }


    //Metodo Apache. Usiamo uno scanner dummy poichè è uno Unit test, e ci "svincoliamo" dalla correttezza e dall'interazione
    //delle altre componenti
    private static class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            //non fa nulla, metodo dummy
            wasProcessCalled = true;

        }
    }

    private static class InvalidJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(entry.toString()));
                reader.readLine();
            } catch (IOException e) {
                if (reader != null) {reader.close();}
                throw new IOException("Eccezione generata dallo scanner non valido");
            }

        }
    }

    private enum Expected {
        IOE, NPE, EXACT_BYTES, NO_READ, PASSED
    }

}
