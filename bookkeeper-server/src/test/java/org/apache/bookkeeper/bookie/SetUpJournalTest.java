package org.apache.bookkeeper.bookie;


import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidLedgerDirsManager;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidServerConfig;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidStatsLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(Parameterized.class)
public class SetUpJournalTest {

    public static volatile boolean uncaughtExceptionInThread = false;
    public static volatile Throwable threadException = null;
    private static boolean busyWait = true;
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    Journal journal;
    int index;
    ServerConfiguration config;
    LedgerDirsManager ledgerDirsManager;
    StatsLogger statsLogger;
    ByteBufAllocator byteBufAllocator;
    TestBookieImplUtil.ExpectedValue expectedValue;
    File directory;

    public SetUpJournalTest(int index, Type dirType, Type configType, Type ledgDirManType, StatsLogger logger, ByteBufAllocator bba, TestBookieImplUtil.ExpectedValue expected) throws IOException {
        this.index = index;
        this.directory = getDirType(dirType);
        this.config = getConfigType(configType, this.directory);
        this.ledgerDirsManager = getLedgerDirsManagerType(ledgDirManType, this.config);
        this.statsLogger = logger;
        this.byteBufAllocator = bba;
        this.expectedValue = expected;
        uncaughtExceptionInThread = false;
        threadException = null;


    }

    @Parameterized.Parameters
    public static Collection<Object> getParams() {
        return Arrays.asList(new Object[][]{
                //index //directory //config //ledgDirsManager   //statsLog         //byteBufAlloc              //Expected
                {-1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED}, //busy wait enabled
                {-1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED}, //busy wait disabled from here on
                {0, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
                {1, Type.INVALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, IO_EXCEPTION},
                {1, Type.VALID, Type.INVALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, RE_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.INVALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, LEDG_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, null, UnpooledByteBufAllocator.DEFAULT, NP_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new InvalidStatsLogger(), UnpooledByteBufAllocator.DEFAULT, RE_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), null, BUFF_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), new InvalidBBA(), RUNTIME_BUFF_EXCEPTION},
                {1, Type.VALID, Type.VALID, null, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, NP_EXCEPTION},



        });

    }


    public static LedgerDirsManager getLedgerDirsManagerType(Type ledgDirManType, ServerConfiguration conf) throws IOException {
        if (ledgDirManType == null) return null;
        else if (ledgDirManType == Type.VALID) {
            return new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
        } else if (ledgDirManType == Type.INVALID) {
            return new InvalidLedgerDirsManager(true); //Altrimenti l'errore si vede solo nel LOG. Bug? Probabilmente no,
            //perchè il metodo sembra pensato così, anche se il messaggio d'errore è molto vago, e appunto, senza guardare il log
            //passa un po' inosservato. Potrebbe sicuramente esserci un check se è la prima volta o no che quel metodo viene invocato
            //per capire se bisognerebbe fare il throw di un eccezione o no, o quantomeno per avere un log più chiaro.
            //"Error doing ... but it's ok if it's the first time opening this journal" dice più o meno il log, che non è chiarissimo.
        }
        return null;
    }

    public static ServerConfiguration getConfigType(Type configType, File dir) throws IOException {

        if (configType == null) return null;
        else if (configType == Type.VALID) {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setJournalDirsName(new String[]{dir.getPath()})
                    .setMetadataServiceUri(null)
                    .setJournalAdaptiveGroupWrites(false)
                    .setMaxBackupJournals(0)
                    .setBusyWaitEnabled(busyWait)
                    .setJournalFlushWhenQueueEmpty(!busyWait) //stesso discorso di busyWait, per semplicità uso una sola variabile
                    .setBookiePort(1);
            busyWait = false; //lo userò solo per la prima run, avevamo un missed branch identificato grazie a Jacoco
            File directory = temporaryFolder.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(directory));
            conf.setLedgerDirNames(directory.list());
            return conf;
        } else if (configType == Type.INVALID) {
            return new InvalidServerConfig();
        }

        fail();
        return null;

    }

    public static File getDirType(Type dirType) throws IOException {
        Random random = new Random();
        if (dirType == null) return null;
        else if (dirType == Type.VALID) {

            File directory = temporaryFolder.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(directory));
            return directory;
        } else if (dirType == Type.INVALID) {
            String path = "/target/tampDirs/journal" + random.nextInt() + "\0";
            return new File(path);
        }
        return null;
    }


    @Test
    public void newJournalTest() throws IOException {
        Exception exceptionRaised = null;

        try {
            this.journal = new Journal(
                    this.index,
                    this.directory,
                    this.config,
                    this.ledgerDirsManager,
                    this.statsLogger,
                    this.byteBufAllocator
            ) {
                @Override
                protected void handleException(Thread t, Throwable e) {
                    System.out.println("There would have been an uncaught exception in the Journal, but I caught it!");
                    System.out.println(e.toString());
                    uncaughtExceptionInThread = true;
                    threadException = e;
                }
            };


            journal.start();
            /*
             * Non è un modo molto elegante di aspettare che il journal parta ma è necessario per evitare di utilizzare
             * il journal mentre sta ancora eseguendo la start. Anche gli sviluppatori Apache implementano (seppur con
             * un meccanismo più complesso) la wait dell'esecuzione dell'avvio del journal, e in alcune occasioni fanno
             * uso direttamente della sleep(...) nei loro test, perciò, anche se è una soluzione sicuramente da migliorare,
             * per questioni di tempo viene lasciata in questo modo.
             */
            Thread.sleep(500);


        } catch (Exception e) {
            exceptionRaised = e;
            assertFalse(uncaughtExceptionInThread);
        }

        if (this.expectedValue == IA_EXCEPTION) {
            assertTrue(exceptionRaised instanceof IllegalArgumentException);
        } else if (this.expectedValue == RE_EXCEPTION) {
            assertTrue(exceptionRaised instanceof RuntimeException);

        } else if (expectedValue == LEDG_EXCEPTION) {
            assertTrue(exceptionRaised instanceof ArrayIndexOutOfBoundsException);
        } else if (expectedValue == NP_EXCEPTION) {
            assertTrue(exceptionRaised instanceof NullPointerException);
        } else if (expectedValue == BUFF_EXCEPTION) {
                /*
                 In questo caso sfruttiamo l'override dell'exception handler per controllare che il journal sarebbe in
                 realtà uscito dalla sua esecuzione (il che però avrebbe interrotto il test, da qui la necessità dell'override).
                 Ci assicuriamo dunque tramite l'assert che l'handler sia stato eseguito.
                 */

            assertTrue(uncaughtExceptionInThread);
            assertTrue(threadException instanceof NullPointerException);


        } else if (expectedValue == RUNTIME_BUFF_EXCEPTION) {
            assertTrue(uncaughtExceptionInThread);
            assertTrue(threadException instanceof IndexOutOfBoundsException);
        }
        //Casi in cui la creazione del Journal non solleva direttamente un'eccezione

        else {
            if (expectedValue == IO_EXCEPTION) {
                //Se c'è stata un eccezione IOException interna, voglio vedere che il journal ha interrotto
                //il suo thread, e che la creazione della sua cartella non è andata a buon fine
                assertFalse(journal.journalDirectory.canRead() || journal.journalDirectory.canWrite() || journal.journalDirectory.exists());
                return;
            }

            assertNull(exceptionRaised);
            assertEquals(this.directory, journal.getJournalDirectory());
            assertTrue(journal.running);
            assertTrue(journal.journalDirectory.canRead() && journal.journalDirectory.canWrite() && journal.journalDirectory.exists());


            Long longNum = 7L; //numero qualsiasi, 7 non ha un significato specifico
            int n = 2;
            for (int i = 0; i < n; i++) {
                File dir = config.getJournalDirs()[0];

                File newFile = new File(dir.getPath(), longNum + ".txn");
                assertTrue(newFile.createNewFile());
                longNum++;
            }
            assertEquals(1 + n, Journal.listJournalIds(config.getJournalDirs()[0], null).size()); //1 file che c'è di "default", più
            //gli n che aggiungo al ciclo for sopra
            for (Long id : Journal.listJournalIds(config.getJournalDirs()[0], null)) {
                journal.setLastLogMark(id, 0);
            }


            try {
                journal.checkpointComplete(journal.newCheckpoint(), true);

            } catch (Exception e) {
                fail("No exception expected here");
            }

            journal.shutdown();

        }


    }

    private enum Type {
        VALID, INVALID
    }

    private static class InvalidBBA extends AbstractByteBufAllocator {

        @Override
        public ByteBuf directBuffer(int i) {

            throw new IndexOutOfBoundsException();

        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }

        @Override
        protected ByteBuf newHeapBuffer(int i, int i1) {
            return null;
        }

        @Override
        protected ByteBuf newDirectBuffer(int i, int i1) {
            return null;
        }

    }


    @After
    public void after() {
        if (journal != null && journal.running)
            journal.shutdown();
    }

}
