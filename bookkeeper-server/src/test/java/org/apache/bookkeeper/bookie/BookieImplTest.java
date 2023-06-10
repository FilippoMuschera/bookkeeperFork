package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.util.testtypes.InputBundle;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BookieImplTest {

    private static final List<File> dirs = new ArrayList<>();
    private final String LEDGER_STRING = "ledger";
    private final int flushInterval = 1000;
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
    private File[] ledgerFiles = new File[] {};
    private final InputBundle bundle;

    //Var statiche per test univariati
    private static boolean hasBookieId = true;

    public BookieImplTest(InputBundle bundle) throws IOException {

        this.bundle = bundle;
        conf.setBookiePort(bundle.bookiePort);
        String[] jourDirs;
        if (bundle.journalDirs == DataType.EMPTY)
            jourDirs = new String[]{};
        else if (bundle.journalDirs == DataType.NULL)
            jourDirs = null;
        else {
            jourDirs = generateTempDirs(3, "journal");
            if (bundle.journalDirs == DataType.INVALID)
                jourDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        String[] ledgDirs;
        if (bundle.ledgDirs == DataType.EMPTY)
            ledgDirs = new String[]{};
        else if (bundle.ledgDirs == DataType.NULL)
            ledgDirs = null;
        else {
            ledgDirs = generateTempDirs(3, LEDGER_STRING);
            if (bundle.journalDirs == DataType.INVALID)
                ledgDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        conf.setJournalDirsName(jourDirs);
        conf.setLedgerDirNames(ledgDirs);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setDiskUsageThreshold(0.99f);
        conf.setFlushInterval(bundle.flushInt);
        conf.setEntryLogPerLedgerEnabled(bundle.logPerLedger);
        if (hasBookieId) {
            conf.setBookieId("localhost:" + bundle.bookiePort);
        }
        reg = bundle.registrationManager;
        ledgerStorage = bundle.ledgerStorage;
        diskChecker = bundle.diskChecker;
        indexDirs = generateIndexDirs(3);
        ledgerDirsManager = this.getCorresponfingLedgerDirsManager(bundle.ledgerDirsManagerType, conf, indexDirs, diskChecker);
        indexDirsManager = this.getCorresponfingLedgerDirsManager(bundle.indexDirsManager, conf, indexDirs, diskChecker);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;


    }

    @Parameterized.Parameters
    public static Collection<InputBundle> getParams() throws IOException {
        Collection<InputBundle> validInputs = new ArrayList<>();
        String il = "org.apache.bookkeeper.bookie.InterleavedLedgerStorage";
       // String sorted = "org.apache.bookkeeper.bookie.SortedLedgerStorage";
        String db = "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage";
        validInputs.add(InputBundle.getDefault());
        validInputs.add(InputBundle.getDefault().setBookiePort(1));
        validInputs.add(InputBundle.getDefault().setLogPerLedger(false).setRegistrationManager(null).setByteBufAllocator(PooledByteBufAllocator.DEFAULT));
        validInputs.add(InputBundle.getDefault().setJournalDirs(DataType.EMPTY).setLedgDirs(DataType.EMPTY));
        validInputs.add(InputBundle.getDefault().setLedgerStorage(LedgerStorageFactory.createLedgerStorage(il)));
        validInputs.add(InputBundle.getDefault().setLedgerStorage(LedgerStorageFactory.createLedgerStorage(db)));



/*        for (int port : new int[]{0, 1}) {
            for (DataType journalDirs : Arrays.asList(DataType.VALID, DataType.EMPTY)) {
                for (DataType ledgDirs : Arrays.asList(DataType.VALID, DataType.EMPTY)) {

                    for (boolean logPerLedger : Arrays.asList(true, false)) {
                        for (RegistrationManager registrationManager : Arrays.asList(null,
                                new NullMetadataBookieDriver.NullRegistrationManager())) {

                            for (LedgerStorage ledgerStorage : Arrays.asList(LedgerStorageFactory.createLedgerStorage(il),
                                    LedgerStorageFactory.createLedgerStorage(sorted), LedgerStorageFactory.createLedgerStorage(db))) {
                                for (DataType ledgerDirManagerType : Collections.singletonList(DataType.VALID)) {
                                    for (DataType indexDirs : Collections.singletonList(DataType.VALID)) {
                                        for (DataType inedxDirsManager : Collections.singletonList(DataType.VALID)) {
                                                InputBundle bundle = new InputBundle(
                                                        port,
                                                        journalDirs,
                                                        ledgDirs,
                                                        10000,
                                                        logPerLedger,
                                                        registrationManager,
                                                        ledgerStorage,
                                                        validDiskChecker,
                                                        ledgerDirManagerType,
                                                        indexDirs,
                                                        inedxDirsManager,
                                                        new NullStatsLogger(),
                                                        UnpooledByteBufAllocator.DEFAULT,
                                                        new SimpleBookieServiceInfoProvider(TestBKConfiguration.newServerConfiguration()),
                                                        ExpectedValue.PASSED

                                                );
                                                validInputs.add(bundle);
                                        }
                                    }
                                }

                            }

                        }
                    }

                }
            }

        }*/


        return validInputs;

    }

    @AfterClass
    public static void delDirs() {
        for (File dir : dirs) {
            dir.delete();
        }
        dirs.clear();
    }

    private static File[] generateIndexDirs(int n) throws IOException {
        File[] indexFiles = new File[n];
        for (int i = 0; i < n; i++) {
            indexFiles[i] = Files.createTempDirectory("index" + i).toFile();
            dirs.add(indexFiles[i]);
        }

        return indexFiles;
    }

    private LedgerDirsManager getCorresponfingLedgerDirsManager(DataType ledgerDirsManagerType, ServerConfiguration conf, File[] indexDirs, DiskChecker diskChecker) throws IOException {
        switch (ledgerDirsManagerType) {
            case NULL:
                return null;
            case VALID:
                return new LedgerDirsManager(conf, indexDirs, diskChecker);
        }
        return null;
    }

    @Before
    public void setUp() throws IOException, InterruptedException, BookieException {

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

    }

    @Test
    public void setUpBookieTest() throws IOException {

        //Serie di assert volti a controllare la correttezza del setup del Bookie

        assertTrue(bookieImpl.isRunning());
        assertFalse(bookieImpl.isReadOnly());
        assertTrue(bookieImpl.getTotalDiskSpace() >= bookieImpl.getTotalFreeSpace());
        assertTrue(bookieImpl.isAvailableForHighPriorityWrites());
        List<File> currDirs = new ArrayList<>(Arrays.asList(BookieImpl.getCurrentDirectories(ledgerFiles)));
        for (File ledgerFile : ledgerFiles) assertTrue(currDirs.contains(BookieImpl.getCurrentDirectory(ledgerFile)));
        if (hasBookieId) {
            assertEquals("localhost:"+conf.getBookiePort(), BookieImpl.getBookieId(conf).toString());
            hasBookieId = false;
        }


    }

    @Test
    public void noWritableLedgerTest() throws IOException, InterruptedException, BookieException, BKException {
        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String mockMsgError = "Error correctly thrown from the mock";

        //Setto i mock necessari. In questa fase di Unit test non abbiamo interesse a testare le interazioni con gli altri
        //oggetti, vogliamo solamente testare BookieImpl in isolamento.

        LedgerDescriptor mockedDescriptor = Mockito.mock(LedgerDescriptorImpl.class);
        Mockito.when(mockedDescriptor.isFenced()).thenReturn(false);
        Mockito.when(mockedDescriptor.getLedgerId()).thenAnswer(invocationOnMock -> {
            throw new LedgerDirsManager.NoWritableLedgerDirException(mockMsgError);
        });
        BookieImpl spiedBookie = Mockito.spy(bookieImpl);
        Mockito.doReturn(mockedDescriptor).when(spiedBookie).getLedgerForEntry(buffer, "mock".getBytes());

        try {
            spiedBookie.addEntry(buffer, true, new BookieImpl.NopWriteCallback(), "foo", "mock".getBytes());
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(mockMsgError)); //Voglio assicurarmi che sia proprio la NoWritableLedgerException lanciata dal mock
            return;
        }
        fail("I should have caught a NoWritableLedgerDirException, but it wasn't thrown!");


    }

    @Test
    public void fenceExceptionTest() throws IOException, InterruptedException, BookieException {
        int ledgerId = 1;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        long entry = 5;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);


        LedgerDescriptor mockedDescriptor = Mockito.mock(LedgerDescriptorImpl.class);
        Mockito.when(mockedDescriptor.isFenced()).thenReturn(true);
        BookieImpl spiedBookie = Mockito.spy(bookieImpl);
        Mockito.doReturn(mockedDescriptor).when(spiedBookie).getLedgerForEntry(buffer, "mock".getBytes());

        try {
            spiedBookie.addEntry(buffer, true, null, "foo", "mock".getBytes());


        } catch (BookieException bookieException) {
            assertEquals(BookieException.Code.LedgerFencedException, bookieException.getCode());
            return;
        }
        fail("I should have caught a BookieException for a fenced ledger, but I didn't");


    }

    @After
    public void after() {

        int exitCode = bookieImpl.shutdown();
        assertEquals(ExitCode.OK, exitCode);
        assertFalse(bookieImpl.isRunning());
    }

    private String[] generateTempDirs(int n, String suffix) throws IOException {
        if (suffix.equals(LEDGER_STRING)) {
            ledgerFiles = new File[n];
        }
        String[] ret = new String[n];
        for (int i = 0; i < n; i++) {
            Path path = Files.createTempDirectory(suffix + i);
            dirs.add(path.toFile());
            ret[i] = path.toFile().getPath();
            if (suffix.equals(LEDGER_STRING)) {
                ledgerFiles[i] = path.toFile();
            }
        }

        return ret;
    }


}


