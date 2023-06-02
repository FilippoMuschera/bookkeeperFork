package org.apache.bookkeeper.bookie;


import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedConstruction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

//@RunWith(Parameterized.class)
public class FormatTest {

    private static int FOLDER_COUNT = 0;
    private static final int NUM_OF_FOLDERS = 4;
    public static final List<File> folderList = new ArrayList<>();
    public static final List<File> journalList = new ArrayList<>();
    public static final List<File> ledgerList = new ArrayList<>();
    public static final List<File> indexList = new ArrayList<>();

    private static final String JOURNAL = "journal";
    private static final String LEDGER = "ledger";
    private static final String INDEX = "index";
    public static final String METADATA_PATH = "metadata-dir";

    //@Parameterized.Parameters
    public static Collection<Object> getParams() {
        //TODO
        return null;
    }

    @BeforeClass
    public static void createDirs() {
        cleanAll(); //Nel caso questo test venisse eseguito dopo quello con i mock
        for (int i = 0; i < NUM_OF_FOLDERS; i++) {
            newTempDir(JOURNAL);
            newTempDir(LEDGER);
            newTempDir(INDEX);

        }
        newTempDir(METADATA_PATH);
        fillFolders();
    }



    private static void fillFolders() {
        for (File directory : folderList) {
            try {
                File tempFile1 = File.createTempFile("tempFile1", ".tmp", directory);
                File tempFile2 = File.createTempFile("tempFile2", ".tmp", directory);
                File tempFile3 = File.createTempFile("tempFile3", ".tmp", directory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }


@AfterClass
public static void after() {
        cleanAll();
}


    public static void deleteDirs() {
        for (File dir : folderList) {
            deleteFolder(dir);
        }

    }

    private static void newTempDir(String category) {
        String folderName = "temp-folder-" + category + FOLDER_COUNT;
        File directory = new File(folderName);
        assertTrue(directory.mkdir());
        FOLDER_COUNT++;
        folderList.add(directory);
        addToCategoryList(directory, category);
    }

    private static void addToCategoryList(File directory, String category) {
        switch (category) {
            case JOURNAL:
                journalList.add(directory);
                break;

            case LEDGER:
                ledgerList.add(directory);
                break;

            case INDEX:
                indexList.add(directory);
                break;

            case METADATA_PATH:
                break;
        }
    }

    private static void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteFolder(file);
                }
            }
        }
        assertTrue(folder.delete());
    }

    public static void cleanAll() {
        deleteDirs();
        folderList.clear();
        journalList.clear();
        ledgerList.clear();
        indexList.clear();
        FOLDER_COUNT = 0;
    }


    @Test
    public void formatTest() {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setJournalDirsName(extractFileNames(journalList));
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
        BookieImpl.format(configuration, false, true);
    }

    public static String[] extractFileNames(List<File> fileList) {
        String[] stringNames = new String[fileList.size()];
        int i = 0;
        for (File file : fileList) {
            stringNames[i] = file.getName();
            i++;
        }
        return stringNames;
    }


}
