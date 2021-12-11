package il.sormeifyoucan;

public class Starter {
    public static void main(String[] args) {

        new CSVSorter().sortCSV("C:\\workspace\\sortmeifyoucan\\src\\main\\resources\\cities.csv",
                "C:\\workspace\\sortmeifyoucan\\dest\\sorted.csv",
                5,8);
    }
}
