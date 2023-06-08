package org.apache.bookkeeper.bookie;

import io.netty.buffer.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.PortManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class BookieImplTest {

    private BookieImpl bookieImpl;
    private final String LEDGER_STRING = "ledger";
    private final List<File> dirs = new ArrayList<>();
    private File[] ledgerFiles;

    private final int flushInterval = 1000;


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
    public void basicBookieTest() throws IOException {

        assertTrue(bookieImpl.isRunning());
        assertFalse(bookieImpl.isReadOnly());
        assertTrue(bookieImpl.getTotalDiskSpace() >= bookieImpl.getTotalFreeSpace());
        assertTrue(bookieImpl.isAvailableForHighPriorityWrites());

    }

    @Test
    public void readWriteLedgerTest() throws IOException, InterruptedException, BookieException {
        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = (long) (Math.random() * 10);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        Object expectedCtx = "foo";

        bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                           BookieId addr, Object ctx) -> {
            assertSame(expectedCtx, ctx);
            assertEquals(ledgerId, ledgerId1);
            assertEquals(entry, entryId1);
        }, "foo", "key".getBytes());
        Thread.sleep((long) (1.2*flushInterval)); //Do tempo al bookie di flushare la nuova entry
        long actualLastAdd = bookieImpl.ledgerStorage.getLastAddConfirmed(ledgerId);
        assertEquals(lastConf, actualLastAdd);


    }

    @Test
    public void fenceExceptionTest() throws IOException, InterruptedException, BookieException {
        long ledgerId = (long) (Math.random() * 10 + 1);
         addAnEntry(ledgerId); //Questa deve andare a buon fine, voglio che venga creato un ledger un ledger
        try {
            bookieImpl.getLedgerStorage().setFenced(ledgerId);
            assertTrue(bookieImpl.getLedgerStorage().isFenced(ledgerId));
            addAnEntry(ledgerId);

        } catch (BookieException bookieException) {
            assertEquals(BookieException.Code.LedgerFencedException, bookieException.getCode());
            return;
        }
        fail("I had to catch a BookieException for a fenced ledger, but I didn't");




    }

    private void addAnEntry(long ledgerId) throws IOException, InterruptedException, BookieException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        long entry = 5;
        long lastConf = (long) (Math.random() * 10);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        Object expectedCtx = "foo";


            bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                               BookieId addr, Object ctx) -> {
                assertSame(expectedCtx, ctx);
                assertEquals(ledgerId, ledgerId1);
                assertEquals(entry, entryId1);
            }, "foo", "key".getBytes());
        Thread.sleep((long) (1.2*flushInterval)); //Do tempo al bookie di flushare la nuova entry
        long actualLastAdd = bookieImpl.ledgerStorage.getLastAddConfirmed(ledgerId);
        assertEquals(lastConf, actualLastAdd);
    }

    @After
    public void after() {
        for (File dir : dirs) {
            dir.delete();
        }
        int exitCode = bookieImpl.shutdown();
        assertEquals(ExitCode.OK, exitCode);
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
        for (int i = 0; i < n; i++){
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
