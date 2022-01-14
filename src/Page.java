import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Page {

    public static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^\"([^\"]*)\" \"([^\"]*)\"$");

    private static int pageWrites = 0;
    private static int pageReads = 0;

    private final RandomAccessFile file;
    private final int pageNumber;
    private final byte[] bytes = new byte[MyDB.PAGE_SIZE];

    private Map<String, String> values;
    private int length;

    Page(RandomAccessFile f, int n) throws IOException {
        file = f;
        pageNumber = n;
        values = new HashMap<>();
        System.out.println("Opening page: " + n);
        if (n > 100) System.exit(1);
        readValues();
    }

    public void put(String key, String value) throws IOException {
        values.put(key, value);
        writeValues();
    }

    public String get(String key) {
        return values.get(key);
    }

    public Map<String, String> getValues() {
        return values;
    }

    private byte[] readBytes() throws IOException {
//        System.out.println("Reading page: " + pageNumber);
        byte[] bytes = new byte[MyDB.PAGE_SIZE];
        file.seek((long) MyDB.PAGE_SIZE * pageNumber);
        file.read(bytes);
        pageReads++;
        return bytes;
    }

    private void readValues() throws IOException {
        String string = new String(readBytes());
        String[] lines = string.split("\n");
        length = lines.length;
        for (String line : lines) {
            Matcher pm = KEY_VALUE_PATTERN.matcher(line);
            if (pm.find()) {
                String key = pm.group(1);
                String value = pm.group(2);
                values.put(key, value);
                length += line.length();
            }
        }
    }

    public void writeValues() throws IOException {
        int i = 0;
        length = 0;
        for (String key : values.keySet()) {
            byte[] lineBytes = String.format("\"%s\" \"%s\"\n", key, values.get(key)).getBytes();
            for (byte b : lineBytes) {
                // TODO: fix bug
                bytes[i] = b;
                i += 1;
            }
            length += lineBytes.length;
        }
        while (i < MyDB.PAGE_SIZE) {
            bytes[i] = 0;
            i += 1;
        }
        writeBytes(bytes);
    }

    private void writeBytes(byte[] bytes) throws IOException {
//        System.out.println("Writing page: " + pageNumber);
        file.seek((long) MyDB.PAGE_SIZE * pageNumber);
        file.write(bytes);
        pageWrites++;
    }

    public void delete(String key) throws IOException {
        values.remove(key);
        writeValues();
    }

    public void clear() throws IOException {
        values.clear();
        writeValues();
    }

    public boolean canPut(String key, String value) {
        return length + key.length() + value.length() + 6 <= MyDB.PAGE_SIZE;
    }

    public void copyTo(Page p2) throws IOException {
        p2.writeBytes(readBytes());
        p2.length = this.length;
        p2.values = this.values;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public static int getPageWrites() {
        return pageWrites;
    }

    public static int getPageReads() {
        return pageReads;
    }
}
