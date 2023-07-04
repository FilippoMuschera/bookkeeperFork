package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.bookie.util.testtypes.InputBundle;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidDiskChecker;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidRegistrationManager;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidStatsLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static org.apache.bookkeeper.bookie.BookieImplTest.generateIndexDirs;
import static org.apache.bookkeeper.bookie.BookieImplTest.getCorrespondingLedgerDirsManager;
import static org.apache.bookkeeper.bookie.util.FolderDeleter.deleteFolder;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType.INVALID;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType.NULL;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BookieImplFailureTest {

    private static final List<File> dirs = new ArrayList<>();
    private final String LEDGER_STRING = "ledger";
    private final InputBundle bundle;
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
    private BookieImpl bookieImpl;
    private File[] ledgerFiles = new File[]{};

    public BookieImplFailureTest(InputBundle bundle) throws IOException {

        this.bundle = bundle;
        conf.setBookiePort(bundle.bookiePort);
        String[] jourDirs;
        if (bundle.journalDirs == TestBookieImplUtil.DataType.EMPTY)
            jourDirs = new String[]{};
        else if (bundle.journalDirs == TestBookieImplUtil.DataType.NULL)
            jourDirs = null;
        else {
            jourDirs = generateTempDirs(3, "journal");
            if (bundle.journalDirs == TestBookieImplUtil.DataType.INVALID)
                jourDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        String[] ledgDirs;
        File[] ledgerDirsFiles = new File[0];
        if (bundle.ledgDirs == TestBookieImplUtil.DataType.EMPTY)
            ledgDirs = new String[]{};
        else if (bundle.ledgDirs == TestBookieImplUtil.DataType.NULL)
            ledgDirs = null;
        else {
            ledgDirs = generateTempDirs(3, LEDGER_STRING);
            ledgerDirsFiles = new File[3];
            for (int i = 0; i < ledgDirs.length; i++) {
                    ledgerDirsFiles[i] =  new File(ledgDirs[i]);

            }
            if (bundle.ledgDirs == TestBookieImplUtil.DataType.INVALID)
                ledgDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

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


        ledgerDirsManager = getCorrespondingLedgerDirsManager(bundle.ledgerDirsManagerType, conf, ledgerDirsFiles, diskChecker);
        indexDirsManager = getCorrespondingLedgerDirsManager(bundle.indexDirsManager, conf, indexDirs, diskChecker);

        if (bundle.indexDirs == INVALID) {
            indexDirs[0] = new File("notAPath\0");
        }
        String[] indexDirsNames = new String[3];
        for (int i = 0; i < indexDirs.length; i++) {
            indexDirsNames[i] = indexDirs[i].getPath();
        }
        conf.setIndexDirName(indexDirsNames);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;




    }


    @Parameterized.Parameters
    public static Collection<InputBundle> getParams() throws IOException {
        List<InputBundle> bundles = new ArrayList<>();
        bundles.add(InputBundle.getDefault().setBookiePort(-1).setExpectedValue(IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setJournalDirs(INVALID).setExpectedValue(IO_EXCEPTION));
        bundles.add(InputBundle.getDefault().setLedgDirs(INVALID).setExpectedValue(IO_EXCEPTION));
        bundles.add(InputBundle.getDefault().setFlushInt(0).setExpectedValue(IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setFlushInt(-1).setExpectedValue(IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setRegistrationManager(new InvalidRegistrationManager()).setExpectedValue(IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setLedgerStorage(null).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setDiskChecker(null).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setDiskChecker(new InvalidDiskChecker(0.99f, 0.98f)).setExpectedValue(DISK_EXCEPTION));
        bundles.add(InputBundle.getDefault().setLedgerDirsManagerType(NULL).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setLedgerDirsManagerType(INVALID).setExpectedValue(IO_EXCEPTION));
        bundles.add(InputBundle.getDefault().setIndexDirs(INVALID).setExpectedValue(IO_EXCEPTION));
        bundles.add(InputBundle.getDefault().setStatsLogger(null).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setStatsLogger(new InvalidStatsLogger()).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setIndexDirsManager(NULL).setExpectedValue(NP_EXCEPTION));
        //NullPointer perchè in realtà non si userà direttamente la cartella non valida, ma si andrà a invocare dir.parent() per cercare la
        //directory che contiene la nostra cartella non valida, e come da documentazione, quel metodo ritornerà null, non potendo
        //chiaramente trovare un parent alla cartella "notAPath\0";
        bundles.add(InputBundle.getDefault().setIndexDirsManager(INVALID).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setBookieServiceInfoSupplier(null).setExpectedValue(NP_EXCEPTION));
        bundles.add(InputBundle.getDefault().setBookiePort(65536).setExpectedValue(IA_EXCEPTION));





        return bundles;
    }


    @Test
    public void setUpBookieTest() throws InterruptedException, BookieException {
        TestBookieImplUtil.ExpectedValue expectedValue;

        try {
            ledgerStorage.initialize(conf, new NullMetadataBookieDriver.NullLedgerManager(), ledgerDirsManager, indexDirsManager,
                    statsLogger, byteBufAllocator);


            this.bookieImpl = new BookieImpl(
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



            bookieImpl.start();
            if (bundle.registrationManager instanceof InvalidRegistrationManager || bundle.bookieServiceInfoSupplier == null) {
                //Se il RegistrationManager non è valido voglio controllare che la creazione del bookie non sia andata
                //a buon fine, che il bookie abbia un exit code (e dunque è stata forzata la sua terminazione), e che
                //l'exit code sia effettivamente quello che ci segnala un errore nella registrazione del bookie, dal momento
                //che dovrà essere il RegistrationManager a sollevare l'eccezione (in un thread diverso da questo, e pertanto non
                //possiamo fare il catch dell'eccezione stessa).
                //Ricade qui anche il test con l'info supplier null, perchè quell'oggetto è utilizzato per la registrazione del bookie
                assertEquals(ExitCode.ZK_REG_FAIL, bookieImpl.getExitCode());
                return;
            }


            //Test sullo stato del Bookie dopo la creazione delle sua istanza
            assertTrue(bookieImpl.isRunning());
            assertFalse(bookieImpl.isReadOnly());
            assertTrue(bookieImpl.getTotalDiskSpace() >= bookieImpl.getTotalFreeSpace());
            assertTrue(bookieImpl.isAvailableForHighPriorityWrites());

            //Simuliamo una pulizia del bookie prima di iniziare a usarlo
            boolean output = BookieImpl.format(bookieImpl.conf, false, true);
            if (!output) throw new IOException();

            //check sulla struttura delle cartelle ledger del bookie
            List<File> currDirs = new ArrayList<>(Arrays.asList(BookieImpl.getCurrentDirectories(ledgerFiles)));
            for (File ledgerFile : ledgerFiles)
                assertTrue(currDirs.contains(BookieImpl.getCurrentDirectory(ledgerFile)));
            //check sulla correttezza del bookieID
            assertTrue(BookieImpl.getBookieId(conf).toString().contains(Integer.toString(conf.getBookiePort())));

            //test dell'aggiunta di una entry nel bookie
            addEntry();

            //Infine chiudiamo il bookie
            bookieImpl.shutdown();



        } catch (IllegalArgumentException iae) {
            expectedValue = TestBookieImplUtil.ExpectedValue.IA_EXCEPTION;
            assertEquals(expectedValue, bundle.expectedValue);
            return;
        } catch (DiskChecker.DiskErrorException dee) {
            expectedValue = TestBookieImplUtil.ExpectedValue.DISK_EXCEPTION;
            assertEquals(expectedValue, bundle.expectedValue);
            return;

        } catch (IOException ioe) {
            expectedValue = TestBookieImplUtil.ExpectedValue.IO_EXCEPTION;
            assertEquals(expectedValue, bundle.expectedValue);
            return;
        } catch (NullPointerException e) {
            expectedValue = TestBookieImplUtil.ExpectedValue.NP_EXCEPTION;
            assertEquals(expectedValue, bundle.expectedValue);
            return;
        }

        fail("temporary fail");

    }


    private void addEntry() throws IOException, InterruptedException, BookieException {
        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String expectedCtx = "foo";


        bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                           BookieId addr, Object ctx) -> {
            assertSame(expectedCtx, ctx);
            assertEquals(ledgerId, ledgerId1);
            assertEquals(entry, entryId1);
        }, "foo", "key".getBytes());

    }

    private String[] generateTempDirs(int n, String suffix) {
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



    @After
    public void after() {

        if (bookieImpl != null && bookieImpl.isRunning())
            bookieImpl.shutdown();

        Mockito.clearAllCaches();
        deleteFolder(new File("./target/tempDirs/"));

    }


}



