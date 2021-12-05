package il.sormeifyoucan;

import il.sormeifyoucan.util.GeneralUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static il.sormeifyoucan.util.GeneralUtils.*;

/**
 * The class contains relevant methods to sort csv file with limited memory resources
 */
public class CSVSorter {

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    /**
     * Main entry-point to sort CSV file
     *
     * @param srcFilePath - unsorted csv file
     * @param dstFilePath - destination(sorted) csv file
     * @param numOfRecordsInMemory - num of records allowed in memory
     */
    public void sortCSV(String srcFilePath, String dstFilePath, int numOfRecordsInMemory) {
        try {
            long lineCount = getLineCount(new File(srcFilePath));

            // 1. Create and sort initial chunks with async execution
            int numOfInitialChunks = (int) (lineCount / numOfRecordsInMemory);

            List<CompletableFuture<List<String>>> futuresSortedChunks = new ArrayList<>();
            for (int i = 0; i <= numOfInitialChunks; i++) {
                int finalI = i; // compiler demand.
                CompletableFuture<List<String>> sortedChunksFuture = CompletableFuture
                        .supplyAsync(() -> sortChunks(getChunk(srcFilePath, numOfRecordsInMemory, finalI)), executorService);
                futuresSortedChunks.add(sortedChunksFuture);
            }

            // wait till all async executors complete
            CompletableFuture.allOf(futuresSortedChunks.toArray(new CompletableFuture[0])).join();

            // 2. Create Initial Sorted Temp Files
            List<CompletableFuture<String>> initialTempFilesFutures = new ArrayList<>();
            futuresSortedChunks.forEach(futureChunk -> {
                try {
                    List<String> chunks = futureChunk.get();
                    CompletableFuture<String> sortedTempFileFuture = CompletableFuture
                            .supplyAsync(() -> writeChunksToTempFile(chunks), executorService);
                    initialTempFilesFutures.add(sortedTempFileFuture);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            // wait till all async executors complete
            CompletableFuture.allOf(initialTempFilesFutures.toArray(new CompletableFuture[0])).join();

            // 3. Merge Sorted Temp Files
            List<CompletableFuture<String>> mergedTempSortedFiles = initialTempFilesFutures;
            while (mergedTempSortedFiles.size() > 1) {
                List<CompletableFuture<String>> intermediateTempSortedFiles = new ArrayList<>();
                for (int i = 0; i < mergedTempSortedFiles.size(); i += 2) {
                    try {
                        if (i+1 >= mergedTempSortedFiles.size()){
                                // case when odd number of temp files, but we merge always 2
                            intermediateTempSortedFiles.add(mergedTempSortedFiles.get(i));
                            break;
                        }
                        String leftFilePath = mergedTempSortedFiles.get(i).get();
                        String rightFilePath = mergedTempSortedFiles.get(i + 1).get();
                        CompletableFuture<String> mergedTempFileFuture = CompletableFuture
                                .supplyAsync(() -> mergeTempFiles(leftFilePath, rightFilePath), executorService);
                        intermediateTempSortedFiles.add(mergedTempFileFuture);
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                CompletableFuture.allOf(intermediateTempSortedFiles.toArray(new CompletableFuture[0])).join();
                mergedTempSortedFiles = intermediateTempSortedFiles;
            }
            try {
                String finalMergedFiles = mergedTempSortedFiles.get(0).get();
                System.out.println(">>>> result before moving file => " + finalMergedFiles);
                Path mvFinalFile = Files.move(Paths.get(finalMergedFiles), Paths.get(dstFilePath));
                if (mvFinalFile != null){
                    System.out.println(">>>> file successfully moved to:  " + dstFilePath);
                    Files.deleteIfExists(Paths.get(finalMergedFiles));
                } else {
                    System.out.println("failed to move file");
                }

            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }


        } catch (IOException e) {
            throw new RuntimeException("Couldn't handle file", e);
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * returns chunks of strings from original csv key-val
     * @param srcFilePath - the absolute path to csv file
     * @param numOfRecords - number of records in chunks
     * @param offset - the offset from which we should extract the chunks
     * @return - chunk of csv records, from provided file
     */
    public List<String> getChunk(String srcFilePath, int numOfRecords, int offset) {

        AtomicInteger chunkIdx = new AtomicInteger(0);

        try (Stream<String> stream = Files.lines(Paths.get(new File(srcFilePath).toURI()))) {
            return stream.skip(numOfRecords * offset)
                    .peek(x -> chunkIdx.incrementAndGet())
                    .takeWhile(x -> chunkIdx.get() <= numOfRecords)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * Sorts list of key=val strings
     * @param chunks - list of key=val strings
     * @return - sorted by key(string value)
     */
    public List<String> sortChunks(List<String> chunks) {
        return chunks.stream()
//                .map(x -> removeSuffixIfExists(x, ","))
                .map(x -> x.split("="))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]))
                .entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.toList());
    }


    /**
     * The method writes chunks to the temp file
     * @param chunks - sorted chunks of csv key=val
     * @return - absolute file name
     */
    public String writeChunksToTempFile(List<String> chunks) {
        if (chunks == null || chunks.size() == 0) {
            throw new IllegalArgumentException("chunks should not be empty");
        }
        File tempFile = null;
        try {
            tempFile = File.createTempFile(GeneralUtils.randomStringWithLength(7), ".tmp");
        } catch (IOException e) {
            throw new RuntimeException("coudn't create a temp file");
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            writer.write(String.join("\r\n", chunks)); // do something with the file we've opened
        } catch (IOException e) {
            throw new RuntimeException("coudn't write a temp file");
        }
        return tempFile.getAbsolutePath();
    }

    /**
     * Performs merge-sort between 2 temp (sorted) files.
     * @param leftFile - first sorted file
     * @param rightFile - second sorted file
     * @return - merged sorted result / absolute path
     */
    public String mergeTempFiles(String leftFile, String rightFile) {

        File destFile = null;
        try {
            destFile = File.createTempFile(GeneralUtils.randomStringWithLength(7), ".tmp");
        } catch (IOException e) {
            throw new RuntimeException("couldn't create a destination temp file");
        }

        try {
            List<String> leftLines = Files.readAllLines(Paths.get(leftFile));
            List<String> rightLines = Files.readAllLines(Paths.get(rightFile));

            int leftOffset = 0;
            int rightOffset = 0;

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(destFile))) {
                while (leftOffset < leftLines.size() && rightOffset < rightLines.size()) {
                    if (keyFromKeyValStr(leftLines.get(leftOffset)).compareTo(keyFromKeyValStr(rightLines.get(rightOffset))) < 0) {
                        writer.append(leftLines.get(leftOffset) + "\r\n");
                        leftOffset++;
                    } else {
                        writer.append(rightLines.get(rightOffset) + "\r\n");
                        rightOffset++;
                    }
                }

                while (leftOffset < leftLines.size()) {
                    writer.append(leftLines.get(leftOffset) + "\r\n");
                    leftOffset++;
                }

                while (rightOffset < rightLines.size()) {
                    writer.append(rightLines.get(rightOffset) + "\r\n");
                    rightOffset++;
                }
            }
            Files.deleteIfExists(Paths.get(leftFile));
            Files.deleteIfExists(Paths.get(rightFile));
        } catch (IOException e) {
            throw new RuntimeException("IO operation with provided files has failed.");
        }

        return destFile.getAbsolutePath();
    }
}
