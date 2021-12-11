import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

class MyDB {
    
    private OutputStream out;
    private final Page[] pages;
    private static final int PAGE_COUNT = 4;
    public static final int PAGE_SIZE = 1024;
    private final RandomAccessFile file;

    public MyDB() throws IOException {
        Path path = Path.of("../my.db");
        file = new RandomAccessFile(path.toString(), "rw");
        if (!Files.exists(path)) initialize();
        pages = new Page[PAGE_COUNT];
        Arrays.fill(pages, null);
    }

    private void initialize() throws IOException {
        byte[] bytes = new byte[PAGE_COUNT * PAGE_SIZE];
        Arrays.fill(bytes, (byte)0);
        file.write(bytes);
    }

    private Page getPage(String key) throws IOException {
        int n = key.hashCode() % PAGE_COUNT;
        if (pages[n] == null) {
            pages[n] = new Page(file, n);
        }
        return pages[n];
    }

    public void put(String key, String value) throws IOException {
        getPage(key).put(key, value);
    }

    public String get(String key) throws IOException {
        return getPage(key).get(key);
    }

    public void delete(String key) throws IOException {
        getPage(key).delete(key);
    }

    public void drop() throws IOException {
        initialize();
    }

}
