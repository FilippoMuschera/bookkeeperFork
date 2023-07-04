package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.bookkeeper.bookie.FormatTest.*;
import static org.apache.bookkeeper.bookie.FormatTest.METADATA_PATH;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.*;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.extractFileNames;
import static org.junit.Assert.assertTrue;

public class FormatExtendedTest {

    @Test
    public void emptyDirTest() {

        cleanAll();
        createDirs();

        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);

        File tempFile = new File("./target/tempDirs/emptyDir");
        assertTrue(tempFile.mkdirs());
        configuration.setJournalDirsName(new String[]{tempFile.getAbsolutePath()});

        boolean output = BookieImpl.format(configuration, true, false);
        assertTrue(output);

    }

    @Test
    public void notADirTest() throws IOException {

        cleanAll();
        createDirs();

        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);

        File tempFile = new File("temp.txt");
        if (tempFile.exists())
            assertTrue(tempFile.delete());
        assertTrue(tempFile.createNewFile());
        configuration.setJournalDirsName(new String[]{tempFile.getAbsolutePath()});

        boolean output = BookieImpl.format(configuration, true, false);
        assertTrue(output);
        assertTrue(tempFile.delete());

    }



}
