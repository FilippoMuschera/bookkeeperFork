package org.apache.bookkeeper.bookie;

import io.netty.buffer.*;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.PortManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class BookieImplTest {

    private final String LEDGER_STRING = "ledger";
    private final List<File> dirs = new ArrayList<>();
    private final int flushInterval = 1000;
    private BookieImpl bookieImpl;
    private File[] ledgerFiles;

    //TODO parametrizzare i diversi tip di oggetti (dove possibile) e i parametri innteri dove non già testati in altre classi
    @Before
    public void setUp() throws IOException, InterruptedException, BookieException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(PortManager.nextFreePort());
        String[] jourDirs = this.generateTempDirs(3, "journal");
        String[] ledgDirs = this.generateTempDirs(3, LEDGER_STRING);
        conf.setJournalDirsName(jourDirs);
        conf.setLedgerDirNames(ledgDirs);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setDiskUsageThreshold(0.99f);
        conf.setFlushInterval(flushInterval);
        conf.setEntryLogPerLedgerEnabled(true);
        RegistrationManager reg = new NullMetadataBookieDriver.NullRegistrationManager();
        LedgerStorage ledgerStorage = LedgerStorageFactory.createLedgerStorage("org.apache.bookkeeper.bookie.SortedLedgerStorage");
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, ledgerFiles, diskChecker);
        File[] indexDirs = this.generateIndexDirs(3);
        LedgerDirsManager indexDirsManager = new LedgerDirsManager(conf, indexDirs, diskChecker);
        StatsLogger statsLogger = new NullStatsLogger();
        ByteBufAllocator byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        Supplier<BookieServiceInfo> bookieServiceInfo = new SimpleBookieServiceInfoProvider(conf);
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
           throw  new LedgerDirsManager.NoWritableLedgerDirException(mockMsgError);
        });
        BookieImpl spiedBookie = Mockito.spy(bookieImpl);
        Mockito.doReturn(mockedDescriptor).when(spiedBookie).getLedgerForEntry(buffer, "mock".getBytes());

        try {
            spiedBookie.addEntry(buffer, true, null, "foo", "mock".getBytes());
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
        for (File dir : dirs) {
            dir.delete();
        }
        int exitCode = bookieImpl.shutdown();
        assertEquals(ExitCode.OK, exitCode);
        assertFalse(bookieImpl.isRunning());
    }

    private File[] generateIndexDirs(int n) throws IOException {
        File[] indexFiles = new File[n];
        for (int i = 0; i < n; i++) {
            indexFiles[i] = Files.createTempDirectory("index" + i).toFile();
            dirs.add(indexFiles[i]);
        }

        return indexFiles;
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


