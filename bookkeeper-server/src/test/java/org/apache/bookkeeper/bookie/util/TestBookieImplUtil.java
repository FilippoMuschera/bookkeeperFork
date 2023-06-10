package org.apache.bookkeeper.bookie.util;

import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.util.PortManager;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static org.apache.bookkeeper.bookie.FormatTest.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBookieImplUtil {

    public static String getInterfaceName(DataType dataType) throws SocketException, UnknownHostException {

        boolean exceptionThrown = false;
        try {
            switch (dataType) {
                case NULL:
                    return null;
                case EMPTY:
                    return "";
                case INVALID:
                    return "notAnInterface";
                case VALID:
                    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                    String address = InetAddress.getLocalHost().getHostAddress();
                    while (interfaces.hasMoreElements()) {
                        NetworkInterface iface = interfaces.nextElement();
                        String host = DNS.getDefaultHost(iface.getName());
                        String ip = new InetSocketAddress(host, PortManager.nextFreePort()).getAddress().getHostAddress();

                        if (address.equals(ip))
                            return iface.getName();
                    }
                    throw new RuntimeException("errore nel reperire la corretta interfaccia di rete");

            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
        return null;
    }


    public enum ExpectedValue {
        PASSED, UH_EXCEPTION, IO_EXCEPTION, NO_SPACE_EXCEPTION, IA_EXCEPTION
    }

    public enum DataType {
        NULL, VALID, INVALID, EMPTY
    }

    public static void createDirs() {
        for (int i = 0; i < NUM_OF_FOLDERS; i++) {
            try {
                newTempDir(JOURNAL);
            }
            catch (NullPointerException e) {
                /*
                 * Se qui si verifica una NPE è perchè il test richiede che la relativa lista di directory sia null.
                 * Quindi questa eccezione è un comportamento previsto, e pertanto non interrompiamo l'esecuzione
                 * nel caso in cui si verifichi.
                 */
            }

            try {newTempDir(LEDGER);}
            catch (NullPointerException e) {
                //come sopra
            }

            try {newTempDir(INDEX);}
            catch (NullPointerException e) {
                //come sopra
            }

        }

        try {newTempDir(METADATA_PATH);}
        catch (NullPointerException e) {
            //come sopra
        }
        fillFolders();
    }

    public static void fillFolders() {
        for (File directory : folderList) {
            try {
                File.createTempFile("tempFile1", ".tmp", directory);
                File.createTempFile("tempFile2", ".tmp", directory);
                File.createTempFile("tempFile3", ".tmp", directory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static void deleteDirs() {
        for (File dir : folderList) {
            deleteFolder(dir);
        }

    }

    public static void newTempDir(String dirSuffix) {
        String folderName = "temp-folder-" + dirSuffix + FOLDER_COUNT;
        File directory = new File(folderName);
        directory.mkdir(); //se è false la cartella è già presente, che è comunque ok
        FOLDER_COUNT++;
        folderList.add(directory);
        addToCategoryList(directory, dirSuffix);
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
        folderList = new ArrayList<>();
        journalList = new ArrayList<>();
        ledgerList = new ArrayList<>();
        indexList = new ArrayList<>();
        FOLDER_COUNT = 0;
    }

    public static String[] extractFileNames(List<File> fileList) {
        String[] stringNames = new String[fileList.size()];
        int i = 0;
        for (File file : fileList) {
            if (file == null)
                stringNames[i] = INVALID_PATH;
            else
                stringNames[i] = file.getName();
            i++;
        }
        return stringNames;
    }







}
