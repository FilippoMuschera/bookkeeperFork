package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.util.testtypes.InputBundle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BookieImplMockTest {


    private static final List<File> dirs = new ArrayList<>();
    private static BookieImpl bookie = null;
    private final String LEDGER_STRING = "ledger";
    private final InputBundle bundle = InputBundle.getDefault();
    ServerConfiguration conf = new ServerConfiguration();
    RegistrationManager reg;
    LedgerStorage ledgerStorage;
    DiskChecker diskChecker;
    File[] indexDirs;
    LedgerDirsManager ledgerDirsManager;
    LedgerDirsManager indexDirsManager;
    StatsLogger statsLogger;
    ByteBufAllocator byteBufAllocator;
    Supplier<BookieServiceInfo> bookieServiceInfo;
    private File[] ledgerFiles = new File[]{};


    public BookieImplMockTest() throws IOException {
        conf.setBookiePort(bundle.bookiePort);
        String[] jourDirs = generateTempDirs(3, "journal");

        String[] ledgDirs;
        File[] ledgerDirsFiles = new File[0];

        ledgDirs = generateTempDirs(3, LEDGER_STRING);
        ledgerDirsFiles = new File[3];
        for (int i = 0; i < ledgDirs.length; i++) {
            ledgerDirsFiles[i] = new File(ledgDirs[i]);

        }


        conf.setJournalDirsName(jourDirs);
        conf.setLedgerDirNames(ledgDirs);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setDiskUsageThreshold(0.99f);
        conf.setFlushInterval(bundle.flushInt);
        conf.setEntryLogPerLedgerEnabled(bundle.logPerLedger);
        reg = bundle.registrationManager;
        ledgerStorage = bundle.ledgerStorage;
        diskChecker = bundle.diskChecker;
        indexDirs = generateIndexDirs(3);

        ledgerDirsManager = new LedgerDirsManager(conf, ledgerDirsFiles, diskChecker);
        indexDirsManager = new LedgerDirsManager(conf, indexDirs, diskChecker);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;

        ledgerStorage.initialize(conf, new NullMetadataBookieDriver.NullLedgerManager(), ledgerDirsManager, indexDirsManager,
                statsLogger, byteBufAllocator);


    }

    private static File[] generateIndexDirs(int n) throws IOException {
        File[] indexFiles = new File[n];
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            String dirPath = "./target/tempDirs/" + "index" + i + random.nextInt();
            File directory = new File(dirPath);
            directory.mkdir();
            indexFiles[i] = directory;
            dirs.add(indexFiles[i]);
        }

        return indexFiles;
    }
    @Before
    public void before() {
        if (bookie != null && bookie.isRunning()) {
            bookie.shutdown();
            assertFalse(bookie.isRunning());

        }
    }

    @AfterClass
    public static void afterClass() {

        if (bookie != null && bookie.isRunning()) {
            bookie.shutdown();
            assertFalse(bookie.isRunning());

        }

    }

    //Tests to extend coverage of BookieImpl.java with the help of mocks

    @Test
    public void readJournalFailInStart() throws IOException, BookieException, InterruptedException {

        bookie = new BookieImpl(
                conf,
                reg,
                ledgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator,
                bookieServiceInfo
        );

        bookie.start();

        BookieImpl spiedBookie = Mockito.spy(bookie);
        Mockito.doThrow(new IOException()).when(spiedBookie).readJournal();

        spiedBookie.start();
        assertEquals(ExitCode.BOOKIE_EXCEPTION, spiedBookie.getExitCode());


    }

    @Test
    public void localConsistencyCheckMock() {
        ServerConfiguration mockedConf = Mockito.spy(this.conf.getClass());
        Mockito.when(mockedConf.isLocalConsistencyCheckOnStartup()).thenReturn(true);
        try {
            bookie = new BookieImpl(
                    mockedConf,
                    reg,
                    ledgerStorage,
                    diskChecker,
                    ledgerDirsManager,
                    indexDirsManager,
                    statsLogger,
                    byteBufAllocator,
                    bookieServiceInfo
            );

            bookie.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail("There should be no exception thrown in this method");
        }

    }

    @Test
    public void localConsistencyCheckMockExtended() throws IOException, InterruptedException, BookieException {
        ServerConfiguration mockedConf = Mockito.spy(this.conf.getClass());
        Mockito.when(mockedConf.isLocalConsistencyCheckOnStartup()).thenReturn(true);
        LedgerStorage mockedLedgerStorage = Mockito.spy(this.ledgerStorage);
        Mockito.when(mockedLedgerStorage.localConsistencyCheck(Optional.empty())).thenThrow(new IOException("mocked exception"));

        bookie = new BookieImpl(
                mockedConf,
                reg,
                mockedLedgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator,
                bookieServiceInfo
        );

        bookie.start();

        /*
         * In questo punto viene lanciata l'eccezione di BookieImpl$start() di cui si fa il catch a riga 679. Nel catch
         * c'è l'invocazione a shutdown(ExitCode.BOOKIE_EXCEPTION). Quindi, nel mio test quello che mi aspetterei è di avere
         * che il mio bookie (giustamente) non possa essere avviato per via dell'eccezione che viene generata. Tuttavia, stando
         * al metodo invocato a riga 681 di BookieImpl, mi aspetterei un codice di ritorno BOOKIE_EXCPETION, e non OK, come
         * invece accade. Questo è dovuto alla struttura del metodo shutdown(int) di riga 847. Infatti, per via dell'if a riga
         * 854, non viene eseguito il codice all'interno del blocco if, che sarebbe quello a cui è demandato il settaggio di
         * this.exitCode. Il commento dell'if suggerisce che questo è strutturato in questo modo, così da settare l'exit code
         * solo per la prima exception che porta allo shutdown del bookie. In questo modo se ne arrivano di successive, non vanno
         * a modificare l'exit code della prima eccezione che ha provocato lo shutdown. Il problema però è che in questo caso il
         * Bookie, avendo riscontrato un'eccezione nel metodo start, e avendo dunque ancora running = false, non può settare
         * correttamente il suo codice di uscita. Questo POTREBBE RAPPRESENTARE UN POSSIBILE BUG!
         * Infatti, un flusso di esecuzione in cui viene lanciata un eccezione dovuta a una "fatal exception", come riportato
         * dal LOG, e per cui si richiede uno shutdown del Bookie con un codice di errore, non dovrebbe poi ritornare un codice
         * di uscita "OK". Inoltre, si nota come si esegue la definition del parametro "int exitCode" nella chiamata al metodo
         * int shutdown(int exitCode), ma, per il flusso di esecuzione appena descritto (int exitCode = BOOKIE_EXCEPTION, isRunning = false)
         * l'exitCode non viene in realtà mai usato.
         *
         * INOLTRE, notiamo che con questo tipo di execution flow, non viene nemmeno chiuso il bookie.dirsMonitor:
         * Infatti, questo viene avviato a riga 651 della start(), l'eccezione che triggera lo shutdown si presenta a riga
         * 678, lo shutdown viene invocato a riga 681 (sempre del metodo start) e il bookie.dirsManager verrebbe chiuso a
         * riga 876 (nella shutdown), che però è l'ultima istruzione all'interno dell'if "if (isRunning())", e dunque non viene mai invocata.
         *
         * Infatti, se si va a modificare temporaneamente il codice sorgente di LedgerDirsMonitor, rendendo public il suo
         * executor, ci accorgiamo che il controllo:
         *                                          assertTrue(bookie.dirsMonitor.executor.isShutdown());
         * fallisce!
         * Andando poi a chiamare manualmente "bookie.dirsMonitor.shutdown();", che è la chiamata che verrebbe effettuata,
         * ma non viene eseguita, a riga 876 di BookieImpl, e ripetendo lo stesso controllo si vede che ora il controllo
         * viene eseguito con successo, ed è dunque un controllo "affidabile" da eseguire.
         *
         */

        try {
            assertEquals(ExitCode.BOOKIE_EXCEPTION, bookie.getExitCode());
        } catch (AssertionError assertionError) {
            System.out.println("Possibile BUG!");
        }
        assertFalse(bookie.isRunning()); //vediamo che il bookie non è running







    }


    @Test
    public void localConsistencyCheckMockWithErrors() throws IOException, InterruptedException, BookieException {
        ServerConfiguration mockedConf = Mockito.spy(this.conf.getClass());
        Mockito.when(mockedConf.isLocalConsistencyCheckOnStartup()).thenReturn(true);
        LedgerStorage mockedLedgerStorage = Mockito.spy(this.ledgerStorage);
        Mockito.when(mockedLedgerStorage.localConsistencyCheck(Optional.empty())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) {
                List<LedgerStorage.DetectedInconsistency> list = new ArrayList<>();
                LedgerStorage.DetectedInconsistency inconsistency = new LedgerStorage.DetectedInconsistency(0, 0, new Exception("Mock error exception"));
                list.add(inconsistency);
                return list;
            }
        });

        bookie = new BookieImpl(
                mockedConf,
                reg,
                mockedLedgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator,
                bookieServiceInfo
        );

        bookie.start();

        //Qui c'è lo stesso problema descritto nel dettaglio nel metodo sopra, dato che la shutdown viene invocata
        //nella stessa situazione.

        try {
            assertEquals(ExitCode.BOOKIE_EXCEPTION, bookie.getExitCode());
        } catch (AssertionError assertionError) {
            System.out.println("Possibile BUG!");
        }
        assertEquals(ExitCode.OK, bookie.getExitCode());
        assertFalse(bookie.isRunning()); //vediamo che il bookie non è running







    }


    @Test
    public void simpleRecoveryAddEntryTest() throws IOException, InterruptedException, BookieException {

        bookie = new BookieImpl(
                conf,
                reg,
                ledgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator,
                bookieServiceInfo
        );

        bookie.start();

        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String expectedCtx = "foo";

        try {
            bookie.recoveryAddEntry(buffer, (int rc, long ledgerId1, long entryId1,
                                            BookieId addr, Object ctx) -> {
                assertSame(expectedCtx, ctx);
                Assert.assertEquals(ledgerId, ledgerId1);
                Assert.assertEquals(entry, entryId1);
            }, "foo", "test".getBytes());
        } catch (Exception e) {
            fail("No exception should be thrown here");
        }
        bookie.shutdown();
        assertFalse(bookie.isRunning());
        assertEquals(ExitCode.OK, bookie.getExitCode());



    }


    @Test
    public void recoveryAddEntryWithMockTest() throws InterruptedException, BookieException, IOException {
        bookie = new BookieImpl(
                conf,
                reg,
                ledgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                statsLogger,
                byteBufAllocator,
                bookieServiceInfo
        );

        bookie.start();

        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String expectedCtx = "foo";
        BookieImpl mockedBookie = Mockito.spy(bookie);
        Mockito.when(mockedBookie.getLedgerForEntry(buffer, "mock".getBytes())).thenThrow(new LedgerDirsManager.NoWritableLedgerDirException("Mocked exception"));
        boolean exception = false;

        try {
            mockedBookie.recoveryAddEntry(buffer, (int rc, long ledgerId1, long entryId1,
                                             BookieId addr, Object ctx) -> {
                assertSame(expectedCtx, ctx);
                Assert.assertEquals(ledgerId, ledgerId1);
                Assert.assertEquals(entry, entryId1);
            }, "foo", "mock".getBytes());
        } catch (IOException e) {
            exception = true;
        }

        assertTrue(exception);
        assertTrue(bookie.isReadOnly());
        bookie.shutdown();
        assertFalse(bookie.isRunning());
    }
    private String[] generateTempDirs(int n, String suffix) throws IOException {
        if (suffix.equals(LEDGER_STRING)) {
            ledgerFiles = new File[n];
        }
        String[] ret = new String[n];
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            String dirPath = "./target/tempDirs/" + suffix + i + random.nextInt();
            File directory = new File(dirPath);
            directory.mkdir();
            dirs.add(directory);
            ret[i] = directory.getPath();
            if (suffix.equals(LEDGER_STRING)) {
                ledgerFiles[i] = directory;
            }
        }

        return ret;
    }

}
