package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.bookkeeper.bookie.ApacheBookieJournalUtil.writeV4Journal;

public class JournalUtil {

    private static BookieImpl bookie;
    private static List<File> tempDirs = new ArrayList<>();
    public static final int NUM_OF_ENTRIES_ON_FIRST_JOURNAL = 200;
    public static final int NUM_OF_ENTRIES_ON_SECOND_JOURNAL = 100;

    public static File tempDir(String dirSuffix) throws IOException {
        String prefix = "temp-folder-";
        File dir = IOUtils.createTempDir(prefix, dirSuffix);
        tempDirs.add(dir);
        return dir;
    }

    public static void shutDownBookie() {
        bookie.shutdown();
    }

    public static void clearDirs() {
            for (File dir : tempDirs) {
                deleteDirectory(dir);
            }
    }

    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }

    public static Journal createJournal() throws Exception {

        File journalDir = tempDir("journal");
        File journalDir2 = tempDir("journal2");

        File ledgerDir = tempDir("ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir2));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), NUM_OF_ENTRIES_ON_FIRST_JOURNAL, "test".getBytes());
        JournalTest.writtenBytes = jc.fc.position();
        writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), NUM_OF_ENTRIES_ON_SECOND_JOURNAL, "test".getBytes());


        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDir.getPath(), journalDir2.getPath()})
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        return bookie.journals.get(0);

    }






}
