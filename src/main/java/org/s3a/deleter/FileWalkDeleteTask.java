package org.s3a.deleter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Stream;

@Slf4j
class FileWalkDeleteTask extends RecursiveAction {
    private Path path;
    private int baseElements;
    private Calendar deleteTime;

    FileWalkDeleteTask(Path path, int baseElements, Calendar deleteTime) {
        this.path = path;
        this.baseElements = baseElements;
        this.deleteTime = deleteTime;
    }

    @Override
    protected void compute() {
        try {
            int elements = this.path.getNameCount() - this.baseElements;
            if (elements < 7) {
                if (Files.isDirectory(this.path)) {
                    try {
                        Collection<FileWalkDeleteTask> subTasks = new HashSet<>();
                        Stream<Path> pathStream = Files.walk(this.path, 1);
                        pathStream.filter(subPath -> (Files.isDirectory(subPath))).forEach(subPath -> {
                            log.debug("Computing path: " + subPath.toString());
                            subTasks.add(new FileWalkDeleteTask(subPath, this.baseElements, this.deleteTime));
                        });
                        invokeAll(subTasks);
                    } catch (IOException e) {
                        e.printStackTrace();
                        log.error("Error walking path from " + this.path, e);
                    }
                    deleteDirectoryIfEmpty(this.path);
                }
            } else {
                if (getTimeFromFullPath(this.path.subpath(2, elements)).before(this.deleteTime)) {
                    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(this.path)) {
                        for (Path entry : directoryStream) {
                            try {
                                Files.delete(entry);
                            } catch (IOException e) {
                                e.printStackTrace();
                                // Log it and move on.  Do as much work as we can.
                                // This may be transient, but even if not, there isn't much else that we can do.
                                log.error("Error deleting path " + entry.toString(), e);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        // Log it and move on.  Do as much work as we can.
                        // This may be transient, but even if not, there isn't much else that we can do.
                        log.error("Error creating directory stream for " + this.path.toString(), e);
                        return;
                    }
                    deleteDirectoryIfEmpty(this.path);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void deleteDirectoryIfEmpty(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            e.printStackTrace();
            // Apparently the directory wasn't empty.
            log.error("Error deleting directory " + path.toString() + " after trying to delete its contents.", e);
        }
    }

    private static Calendar getTimeFromFullPath(Path datePath) {
        Calendar time = Calendar.getInstance();
        time.clear();
        Integer year = Integer.parseInt(datePath.getName(0).toString());
        Integer month = Integer.parseInt(datePath.getName(1).toString());
        Integer day = Integer.parseInt(datePath.getName(2).toString());
        Integer hour = Integer.parseInt(datePath.getName(3).toString());
        Integer min = Integer.parseInt(datePath.getName(4).toString());
        time.set(year, month - 1, day, hour, min);
        // get the last second of the current minute
        time.add(Calendar.MINUTE, 1);
        time.add(Calendar.SECOND, -1);
        return time;
    }
}
