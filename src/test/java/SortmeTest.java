import il.sormeifyoucan.CSVSorter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static il.sormeifyoucan.util.GeneralUtils.getLineCount;
import static il.sormeifyoucan.util.GeneralUtils.randomStringWithLength;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SortmeTest {

    CSVSorter csvSorter = new CSVSorter();


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
        List lst = new ArrayList();
        lst.add("zzz=123");
        lst.add("yyy=ooo");
        lst.add("abc=abc");
        lst.add("aaa=bbbb");
        lst.add("cba=cba");
        List sortedChunks = csvSorter.sortChunks(lst);
        assertTrue(sortedChunks.get(0).equals("aaa=bbbb"));
        assertTrue(sortedChunks.get(lst.size()-1).equals("zzz=123"));
    }

    @Test
    public void createCSVFileAndSortItEndToEnd() {
        int numOfRecords = 103;
        String file = csvSorter.writeChunksToTempFile(createRandomCsv(numOfRecords));
        System.out.println(">>> Not Sorted File: " + file);
        csvSorter.sortCSV(file,"./dest/sorted.csv",5);
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
