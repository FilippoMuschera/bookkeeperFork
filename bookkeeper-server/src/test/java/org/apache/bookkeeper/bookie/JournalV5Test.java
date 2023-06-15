package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.bookkeeper.bookie.JournalUtil.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * Aggiungiamo questo test per estendere la coverage del metodo scanJournal(). Dal report JaCoCo si vede infatti che il
 * journal in versione 4 non scrive mai byte di padding, e di conseguenza la parte del metodo di scanJournal che dovrebbe
 * "skipparli" quando presente non è mai coperta da test.
 */
public class JournalV5Test {

    Journal journal;
    long journalID;
    public static long WRITTEN_BYTES_V5;
    private static long NUM_OF_ENTRIES = 0;
    @Before
    public void before() throws Exception {
        this.journal = createJournalV5();
        this.journalID = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        //ora writtenBytes ha il numero di byte scritti sul Journal, bisogna aggiungere quelli relativi all'update
        //della versione del journal stesso (che è un int).
        WRITTEN_BYTES_V5 += Integer.BYTES;




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
            readBytes = this.journal.scanJournal(this.journalID, 0, new DummyJournalScan());


        } catch (IOException | NullPointerException e) {
            fail("There was exception, none was expected while testing journal v5");
        }
        assertEquals(WRITTEN_BYTES_V5, readBytes);
        //Il "-2" è per rimuovere le entry con i metadati (la prima) e quella con la JournalVersion (l'ultima)
        //E contare quindi solo le entry di cui abbiamo richiesto la scrittura.
        assertEquals(NUM_OF_ENTRIES - 2, NUM_OF_ENTRIES_ON_JOURNAL);
        System.out.println(NUM_OF_ENTRIES + ", " + WRITTEN_BYTES_V5);


    }


    /*
     * In questa versione del test contiamo quante volte viene invocato il metodo process del nostro scanner dummy perchè
     * vogliamo assicurarci che il numero di entry sia esattamente quello che ci aspettiamo, e che non vengano erroneamente
     * contate come entry (e quindi come contenuto vero e proprio del Journal) i byte di Padding che vengono inseriti durante
     * una scrittura sul Journal in V5.
     * Meccanismo implementato per migliorare la qualità del test sulla base del report di PIT.
     */
    private static class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {

            NUM_OF_ENTRIES += 1;

        }
    }

}
