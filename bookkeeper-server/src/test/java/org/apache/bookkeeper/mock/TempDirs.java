package org.apache.bookkeeper.mock;

import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class TempDirs {
        private final List<File> tmpDirs = new LinkedList<>(); // retained to delete files

        public File createNew(String prefix, String suffix) throws Exception {
                File dir = IOUtils.createTempDir(prefix, suffix);
                tmpDirs.add(dir);
                return dir;
        }

        public void cleanup() throws Exception {
                for (File f : tmpDirs) {
                        FileUtils.deleteDirectory(f);
                }
        }

        public List<File> getDirs() {
                return tmpDirs;
        }
}
