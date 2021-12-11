import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Page {

    public static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^\"([^\"]*)\" \"([^\"]*)\"$");

    private final RandomAccessFile file;
    private final int pageNumber;
    private final Map<String, String> values;
    private final byte[] bytes = new byte[MyDB.PAGE_SIZE];

    Page(RandomAccessFile f, int n) throws IOException {
        file = f;
        pageNumber = n;
        values = new HashMap<>();
        readValues();
    }

    public void put(String key, String value) throws IOException {
        values.put(key, value);
        writeValues();
    }

    public String get(String key) {
        return values.get(key);
    }

    private void readValues() throws IOException {
        byte[] bytes = new byte[MyDB.PAGE_SIZE];
        file.seek((long) MyDB.PAGE_SIZE * pageNumber);
        file.read(bytes);
        String string = new String(bytes);
        String[] lines = string.split("\n");
        for (String line : lines) {
            Matcher pm = KEY_VALUE_PATTERN.matcher(line);
            if (pm.find()) {
                String key = pm.group(1);
                String value = pm.group(2);
                values.put(key, value);
            }
        }
    }

    private void writeValues() throws IOException {
        int i = 0;
        for (String key : values.keySet()) {
            byte[] lineBytes = String.format("\"%s\" \"%s\"\n", key, values.get(key)).getBytes();
            for (byte b : lineBytes) {
                bytes[i] = b;
                i += 1;
            }
        }
        while (i < MyDB.PAGE_SIZE) {
            bytes[i] = 0;
            i += 1;
        }
        file.seek((long) MyDB.PAGE_SIZE * pageNumber);
        file.write(bytes);
    }

    public void delete(String key) throws IOException {
        values.remove(key);
        writeValues();
    }
}
