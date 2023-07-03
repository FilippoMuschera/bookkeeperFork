package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

import static org.apache.bookkeeper.bookie.FormatTest.*;
import static org.apache.bookkeeper.bookie.util.FolderDeleter.deleteFolder;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FormatMockTest {


    @Before
    public void cleanBefore(){cleanAll();}

    @After
    public void cleanAfter(){
        cleanAll();
        deleteFolder(new File(METADATA_PATH));
    }


    @Test
    public void formatTestWithMockTrue() {

        try(MockedStatic<IOUtils> mockedStatic = Mockito.mockStatic(IOUtils.class)) {
            mockedStatic.when(() -> IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenReturn(true);

            cleanAll();
            createDirs();

            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setJournalDirsName(extractFileNames(journalList));
            configuration.setLedgerDirNames(extractFileNames(ledgerList));
            configuration.setIndexDirName(extractFileNames(indexList));
            configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
            boolean output = BookieImpl.format(configuration, true, false);
            assertTrue(output);
        }





    }

    @Test
    public void formatTestWithMockFalse() {

        try(MockedStatic<IOUtils> mockedStatic = Mockito.mockStatic(IOUtils.class)) {
            mockedStatic.when(() -> IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenReturn(false);

            cleanAll();
            createDirs();

            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setJournalDirsName(extractFileNames(journalList));
            configuration.setLedgerDirNames(extractFileNames(ledgerList));
            configuration.setIndexDirName(extractFileNames(indexList));
            configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
            boolean output = BookieImpl.format(configuration, true, false);
            assertFalse(output);

        }





    }

    @Test
    public void formatTestWithMockException() {

        /*
         In questo test, come si vede dal mock, viene lanciata una IOException (la lanciamo da IOUtils perchè è un oggetto
         di BookKeeper, anche se lo scenario in cui si potrebbe verificare realmente è quello in cui questa eccezione è
         lanciata da System.in.read(), che però non essendo un tipo che "possediamo", evitiamo di mockare.
         BookieImpl.format() provvede a fare il catch dell'eccezione e ritornare semplicemente false.
         Come si vede infatti dalla segnatura di format(), non esegue il throws di nessuna eccezione.
         */

        try(MockedStatic<IOUtils> mockedStatic = Mockito.mockStatic(IOUtils.class)) {
            mockedStatic.when(() -> IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenThrow(IOException.class);

            cleanAll();
            createDirs();

            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setJournalDirsName(extractFileNames(journalList));
            configuration.setLedgerDirNames(extractFileNames(ledgerList));
            configuration.setIndexDirName(extractFileNames(indexList));
            configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
            boolean output = BookieImpl.format(configuration, true, false);
            assertFalse(output);
        }







    }


}
