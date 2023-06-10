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
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
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
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType.EMPTY;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.PASSED;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.NO_SPACE_EXCEPTION;
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
        if (bundle.journalDirs == EMPTY)
            jourDirs = new String[]{};
        else if (bundle.journalDirs == DataType.NULL)
            jourDirs = null;
        else {
            jourDirs = generateTempDirs(3, "journal");
            if (bundle.journalDirs == DataType.INVALID)
                jourDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        String[] ledgDirs;
        if (bundle.ledgDirs == EMPTY)
            ledgDirs = new String[]{};
        else if (bundle.ledgDirs == DataType.NULL)
            ledgDirs = null;
        else {
            ledgDirs = generateTempDirs(3, LEDGER_STRING);
            if (bundle.ledgDirs == DataType.INVALID)
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

        if (bundle.indexDirs == EMPTY)
            indexDirs = new File[]{};
        else if (bundle.indexDirs == DataType.NULL)
            indexDirs = null;
        else {
            indexDirs = generateIndexDirs(3);
        }
        ledgerDirsManager = getCorrespondingLedgerDirsManager(bundle.ledgerDirsManagerType, conf, indexDirs, diskChecker);
        indexDirsManager = getCorrespondingLedgerDirsManager(bundle.indexDirsManager, conf, indexDirs, diskChecker);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;


    }

    @Parameterized.Parameters
    public static Collection<InputBundle> getParams() throws IOException {
        Collection<InputBundle> validInputs = new ArrayList<>();
        String il = "org.apache.bookkeeper.bookie.InterleavedLedgerStorage";
        String db = "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage";
        validInputs.add(InputBundle.getDefault());
        validInputs.add(InputBundle.getDefault().setBookiePort(1));
        validInputs.add(InputBundle.getDefault().setLogPerLedger(false).setRegistrationManager(null).setByteBufAllocator(PooledByteBufAllocator.DEFAULT));
        validInputs.add(InputBundle.getDefault().setJournalDirs(EMPTY).setLedgDirs(EMPTY).setIndexDirs(EMPTY).setExpectedValue(NO_SPACE_EXCEPTION));
        validInputs.add(InputBundle.getDefault().setLedgerStorage(LedgerStorageFactory.createLedgerStorage(il)));
        validInputs.add(InputBundle.getDefault().setLedgerStorage(LedgerStorageFactory.createLedgerStorage(db)));




        return validInputs;

    }


    public static File[] generateIndexDirs(int n) throws IOException {
        File[] indexFiles = new File[n];
        for (int i = 0; i < n; i++) {
            indexFiles[i] = Files.createTempDirectory("index" + i).toFile();
            dirs.add(indexFiles[i]);
        }

        return indexFiles;
    }

    public static LedgerDirsManager getCorrespondingLedgerDirsManager(DataType ledgerDirsManagerType, ServerConfiguration conf, File[] indexDirs, DiskChecker diskChecker) throws IOException {
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
        if(bundle.ledgDirs == EMPTY && bundle.ledgDirs == EMPTY && bundle.indexDirs == EMPTY) //se non ha cartelle mi aspetto che sia un bookie su cui non posso scrivere
            assertTrue(bookieImpl.isReadOnly());
        else
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

    @Test
    public void addEntryExceptionTest() {
        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String expectedCtx = "foo";

        try {
            bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                               BookieId addr, Object ctx) -> {
                assertSame(expectedCtx, ctx);
                assertEquals(ledgerId, ledgerId1);
                assertEquals(entry, entryId1);
            }, "foo", "key".getBytes());

        } catch (Exception e) {
            assertEquals(NO_SPACE_EXCEPTION, bundle.expectedValue);
            return;
        }
        assertEquals(PASSED, bundle.expectedValue);





    }

    @After
    public void after() throws IOException {

        int exitCode = bookieImpl.shutdown();
        assertEquals(ExitCode.OK, exitCode);
        assertFalse(bookieImpl.isRunning());
        for (File dir : dirs) {
            FileUtils.deleteDirectory(dir);

        }
        try {
            FileUtils.deleteDirectory(new File("/tmp/bk-txn"));

        } catch (Exception e) {
            //just go on, since line in the try is not really cross-system compatible
            e.printStackTrace();
        }
        dirs.clear();
        Mockito.clearAllCaches();
        //System.gc();

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


