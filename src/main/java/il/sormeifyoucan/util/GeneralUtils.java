package il.sormeifyoucan.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.stream.Stream;

public class GeneralUtils {

    /**
     * generates random alpha-numeric string in the given length
     * @param length
     * @return
     */
    public static String randomStringWithLength(int length){
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    /**
     * removes suffix if it exists, otherwise original string
     * @param str
     * @param suffix
     * @return
     */
    public static String removeSuffixIfExists(String str, String suffix) {
        return str.endsWith(suffix)
                ? str.substring(0, str.length() - suffix.length())
                : str;
    }

    /**
     * return the key in a key=val string
     * for abc=123, will return abc
     * @param keyVal
     * @return
     */
    public static String keyFromKeyValStr(String keyVal){
        String[] split = keyVal.split("=");
        if (split.length != 2) throw new RuntimeException("Bad Input: " + keyVal);

        return split[0];
    }

    /**
     * returns number of lines in the file
     * @param file
     * @return
     * @throws IOException
     */
    public static long getLineCount(File file) throws IOException {
        try (Stream<String> lines = Files.lines(file.toPath())) {
            return lines.count();
        }
    }
}
