package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidLedgerDirsManager;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidServerConfig;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.core.util.SystemMillisClock;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(Parameterized.class)
public class SetUpJournalTest {

    Journal journal;
    int index;
    ServerConfiguration config;
    LedgerDirsManager ledgerDirsManager;
    StatsLogger statsLogger;
    ByteBufAllocator byteBufAllocator;
    TestBookieImplUtil.ExpectedValue expectedValue;
    File directory;
    private static Class<? extends Throwable> exceptionInThread;

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public SetUpJournalTest(int index, Type dirType, Type configType, Type ledgDirManType, StatsLogger logger, ByteBufAllocator bba, TestBookieImplUtil.ExpectedValue expected) throws IOException {
        this.index = index;
        this.directory = getDirType(dirType);
        this.config = getConfigType(configType, this.directory);
        this.ledgerDirsManager = getLedgerDirsManagerType(ledgDirManType, this.config);
        this.statsLogger = logger;
        this.byteBufAllocator = bba;
        this.expectedValue = expected;


    }

    @Parameterized.Parameters
    public static Collection<Object> getParams() {
        return Arrays.asList(new Object[][]{
                //index //directory //config //ledgDirsManager   //statsLog         //byteBufAlloc              //Expected
                {-1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
//                {0, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
//                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
                {1, Type.INVALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, IO_EXCEPTION},
                {1, Type.VALID, Type.INVALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, RE_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.INVALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, LEDG_EXCEPTION}





        });

    }

    public static LedgerDirsManager getLedgerDirsManagerType(Type ledgDirManType, ServerConfiguration conf) throws IOException {
        if (ledgDirManType == null) return null;
        else if (ledgDirManType == Type.VALID) {
            return new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
        } else if (ledgDirManType == Type.INVALID) {
            return new InvalidLedgerDirsManager();
        }
        return null;
    }

    public static ServerConfiguration getConfigType(Type configType, File dir) throws IOException {

        if (configType == null) return null;
        else if (configType == Type.VALID) {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setJournalDirsName(new String[] {dir.getPath()})
                    .setMetadataServiceUri(null)
                    .setJournalAdaptiveGroupWrites(false)
                    .setBookiePort(1);
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
                );

                journal.start();
                /*
                 * Non è un modo molto elegante di aspettare che il journal parta ma è necessario per evitare di utilizzare
                 * il journal mentre sta ancora eseguendo la start. Anche gli sviluppatori Apache implementano (seppur con
                 * un meccanismo più complesso) la wait dell'esecuzione dell'avvio del journal
                 */
                Thread.sleep(500);


            } catch (Exception e) {
                exceptionRaised = e;
            }

            if (this.expectedValue == IA_EXCEPTION) {
                assertTrue(exceptionRaised instanceof RuntimeException);
                return;
            } else if (this.expectedValue == RE_EXCEPTION) {
                assertTrue(exceptionRaised instanceof RuntimeException);
                return;
            } else {
                if (expectedValue == IO_EXCEPTION) {
                    //Se c'è stata un eccezione IOException interna, voglio vedere che il journal ha interrotto
                    //il suo thread, e che la creazione della sua cartella non è andata a buon fine
                    assertFalse(journal.journalDirectory.canRead() || journal.journalDirectory.canWrite() || journal.journalDirectory.exists());
                    return;
                }
                if (expectedValue == LEDG_EXCEPTION) {
                    journal.getLastLogMark();
                    //TODO VEDI PER COSA SI USA LE LEDGER DIR E PROVA A FARCI QUALCOSA CHE LA FA CRASHARE
                    return;
                }
                assertNull(exceptionRaised);
                assertEquals(this.directory, journal.getJournalDirectory());
                assertTrue(journal.running);
                assertTrue(journal.journalDirectory.canRead() && journal.journalDirectory.canWrite() && journal.journalDirectory.exists());



                 CheckpointSource.Checkpoint cp = journal.newCheckpoint();
                 journal.checkpointComplete(cp, true);
                System.out.println(journal.journalDirectory.exists());

                 journal.shutdown();

            }




    }

    private enum Type {
        VALID, INVALID
    }


}
