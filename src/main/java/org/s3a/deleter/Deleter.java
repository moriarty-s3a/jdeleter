package org.s3a.deleter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

@Slf4j
public class Deleter {
    private String baseDir;

    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("You must provide the base directory.");
        }
        Deleter deleter = new Deleter(args[0]);
        deleter.execute();
    }

    public Deleter(String baseDir) {
        this.baseDir = baseDir;
    }

    public void execute() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(this.getClass().getClassLoader().getResource("config.json"), Config.class);
        if (config == null || config.getDefaultConfig() == null) {
            throw new IllegalArgumentException("Cannot read configuration or configuration does not have a default configuration listed.");
        }

        Path basePath = Paths.get(baseDir);
        if (!Files.exists(basePath) || !Files.isDirectory(basePath)) {
            throw new IllegalArgumentException("Cannot access the base directory.");
        }
        Map<String, CompanyConfig> configMap = convertConfigToMap(config);
        walkTreeAsStream(basePath, configMap);
        // This version seems to work, but takes forever and destroys the stack.
//        walkTreeWithForkJoin(basePath, configMap);
    }

    private static void walkTreeAsStream(Path basePath, Map<String, CompanyConfig> configMap) throws IOException {
        Stream<Path> pathStream = Files.walk(basePath);
        // forEachOrdered() guarantees that we don't duplicate work, e.x. delete 2010/10 and then delete 2010/ making this
        // potentially faster than forEach() because we have to walk the tree to recursively delete the entire directory structure.
        // If this were production code, we should do some empirical testing on larger datasets than I used before choosing between the two.
        pathStream.filter(path -> (Files.exists(path) && Files.isDirectory(path) && basePath.relativize(path).getNameCount() > 2)).forEachOrdered(path -> {
            log.debug("Path = " + path.toString());
            Path relativePath = basePath.relativize(path);
            int elements = relativePath.getNameCount();
            String companyId = relativePath.getName(0).toString();
            CompanyConfig config = configMap.get(companyId);
            if (config == null) {
                config = configMap.get("default");
            }
            Calendar deleteTime = Calendar.getInstance();
            deleteTime.add(Calendar.DAY_OF_MONTH, -1 * config.getRetention());
            log.debug("Company id = " + companyId);
            Path datePath = relativePath.subpath(2, elements);
            Calendar time = getTimeFromPath(datePath);
            if (time.before(deleteTime)) {
                try {
                    // Since Java NIO doesn't allow us to recursively delete directories...
                    Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException ex) throws IOException {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    // Log it and move on.  Do as much work as we can.
                    // This may be transient, but even if not, there isn't much else that we can do.
                    log.error("Error deleting path " + path.toString(), e);
                }
            }
        });
    }

    private static Calendar getTimeFromPath(Path datePath) {
        int elements = datePath.getNameCount();
        Calendar time = Calendar.getInstance();
        time.clear();
        Integer year = Integer.parseInt(datePath.getName(0).toString());
        time.set(Calendar.YEAR, year);
        if (elements == 1) {
            time.add(Calendar.YEAR, 1);
            time.add(Calendar.SECOND, -1);
            return time;
        }
        Integer month = Integer.parseInt(datePath.getName(1).toString());
        time.set(Calendar.MONTH, month - 1);
        if (elements == 2) {
            time.add(Calendar.MONTH, 1);
            time.add(Calendar.SECOND, -1);
            return time;
        }
        Integer day = Integer.parseInt(datePath.getName(2).toString());
        time.set(Calendar.DAY_OF_MONTH, day);
        if (elements == 3) {
            time.add(Calendar.DAY_OF_MONTH, 1);
            time.add(Calendar.SECOND, -1);
            return time;
        }
        Integer hour = Integer.parseInt(datePath.getName(3).toString());
        time.set(Calendar.HOUR_OF_DAY, hour);
        if (elements == 4) {
            time.add(Calendar.HOUR_OF_DAY, 1);
            time.add(Calendar.SECOND, -1);
            return time;
        }
        Integer min = Integer.parseInt(datePath.getName(4).toString());
        time.set(Calendar.MINUTE, min);
        // get the last second of the current minute
        time.add(Calendar.MINUTE, 1);
        time.add(Calendar.SECOND, -1);
        return time;
    }

    private static void walkTreeWithForkJoin(Path basePath, Map<String, CompanyConfig> configMap) throws IOException {
        Stream<Path> pathStream = Files.walk(basePath, 1);
        ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        // We only walk to a depth of 1 here, so ordering is irrelevant compared to the streaming method.
        pathStream.filter(path -> {
            Path relativePath = basePath.relativize(path);
            return (Files.exists(path) && Files.isDirectory(path) && relativePath.getNameCount() == 1 && !"".equals(relativePath.getName(0).toString()));
        }).forEach(path -> {
            log.debug("Path = " + path.toString());
            Path relativePath = basePath.relativize(path);
            String companyId = relativePath.getName(0).toString();
            CompanyConfig config = configMap.get(companyId);
            if (config == null) {
                config = configMap.get("default");
            }
            Calendar deleteTime = Calendar.getInstance();
            deleteTime.add(Calendar.DAY_OF_MONTH, -1 * config.getRetention());
            log.debug("Company id = " + companyId);
            long start = System.currentTimeMillis();
            pool.invoke(new FileWalkDeleteTask(path, path.getNameCount(), deleteTime));
            long end = System.currentTimeMillis();
            log.debug("Deletion took: " + (end - start));
        });
    }

    private Map<String, CompanyConfig> convertConfigToMap(Config config) {
        Map<String, CompanyConfig> configMap = new HashMap<>();
        configMap.put("default", config.getDefaultConfig());
        for (CompanyConfig companyConfig : config.getCompanies()) {
            configMap.put(companyConfig.getId(), companyConfig);
        }
        return configMap;
    }
}
