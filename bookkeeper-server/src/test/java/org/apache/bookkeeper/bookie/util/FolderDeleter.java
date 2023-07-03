package org.apache.bookkeeper.bookie.util;

import java.io.File;

public class FolderDeleter {

    public static void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteFolder(file);
                }
            }
        }

        try {
            if (folder.exists()) {
                folder.delete();
            }
        } catch (Exception e) {
            //vai avanti
        }
    }


}
