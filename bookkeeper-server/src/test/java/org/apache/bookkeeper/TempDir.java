package org.apache.bookkeeper;

import lombok.Getter;
import org.apache.bookkeeper.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class TempDir {

    @Getter
    private final Path rootPath;
    @Getter
    private final File rootDir;
    public TempDir(String rootPath) throws IOException {
        this.rootPath = Files.createTempDirectory(rootPath);
        this.rootDir = this.rootPath.toFile();

    }
    public File createTempFile (String prefix, String suffix) throws IOException {
        return File.createTempFile(prefix, suffix, this.rootDir);
    }


    public void cleanup() throws IOException {
        AtomicInteger failCounter = new AtomicInteger(0);
        try (Stream<Path> walk = Files.walk(this.rootPath)){
                walk.map(Path::toFile)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(f -> {
                    if (!f.delete()){
                        failCounter.incrementAndGet();
                    }
                });
                if (failCounter.get() > 0){
                    throw new IOException("some file not deleted");
                }
        }
    }




}
