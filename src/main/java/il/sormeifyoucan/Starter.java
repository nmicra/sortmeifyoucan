package il.sormeifyoucan;

public class Starter {
    public static void main(String[] args) {
        new CSVSorter().sortCSV("C:\\temp\\sortmeifyoucan\\src\\main\\resources\\cities.csv",
                "C:\\temp\\sortmeifyoucan\\dest\\sorted.csv",
                5,8);
    }
}
