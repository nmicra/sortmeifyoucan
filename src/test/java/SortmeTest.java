import il.sormeifyoucan.CSVSorter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static il.sormeifyoucan.util.GeneralUtils.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SortmeTest {

    CSVSorter csvSorter = new CSVSorter();


    @Test
    public void testChunksFromFile() throws IOException {
        List<String> chunk1 = csvSorter.getChunk("C:\\workspace\\sortmeifyoucan\\src\\main\\resources\\cities.csv", 8, 1);
        List<String> chunk2 = csvSorter.getChunk("C:\\workspace\\sortmeifyoucan\\src\\main\\resources\\cities.csv", 10, 2);

        assertTrue(chunk1.size() == 8);
        assertTrue(chunk2.size() == 10);
        chunk1.forEach(System.out::println);
        System.out.println("========");
        chunk2.forEach(System.out::println);
    }

    @Test
    public void createChunksAndWriteItToTheFile() throws IOException {
        int numOfRecords = 103;
        String file = csvSorter.writeChunksToTempFile(createRandomCsv(numOfRecords));
        assertTrue(Files.exists(Paths.get(file)));
        assertTrue(getLineCount(new File(file)) == numOfRecords);
        Files.deleteIfExists(Paths.get(file));
    }

    @Test
    public void sortChunksTest() {
        List<String> lst = new ArrayList();
        lst.add("   46,   35,   59, \"N\",    120,   30,   36, \"W\", \"Yakima\", WA");
        lst.add("36,    5,   59, \"N\",     80,   15,    0, \"W\", \"Winston-Salem\", NC");
        lst.add("38,   42,   35, \"N\",     93,   13,   48, \"W\", \"Sedalia\", MO");
        lst.add("39,   11,   23, \"N\",     78,    9,   36, \"W\", \"Winchester\", VA");
        lst.add("41,    9,   35, \"N\",     81,   14,   23, \"W\", \"Ravenna\", OH");
        List<String> sortedChunks = csvSorter.sortChunks(lst,8);

        assertTrue(getColumnStringFromCSVSingleLine(sortedChunks.get(0),9).equals("OH"));
        assertTrue(getColumnStringFromCSVSingleLine(sortedChunks.get(lst.size()-1),9).equals("WA"));
    }

    @Test
    @Disabled
    public void createCSVFileAndSortItEndToEnd() {
        int numOfRecords = 103;
        String file = csvSorter.writeChunksToTempFile(createRandomCsv(numOfRecords));
        System.out.println(">>> Not Sorted File: " + file);
        csvSorter.sortCSV(file,"./dest/sorted.csv",5,8);
        System.out.println(">>> End.");
    }




    private List<String> createRandomCsv(int n) {
        List<String> lst = new ArrayList<>();
        for (int i = 0; i < n; i++){
            lst.add(randomStringWithLength(7) + "=" + randomStringWithLength(5) + ",");
        }
        return lst;
    }
}
