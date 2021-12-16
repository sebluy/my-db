import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

class MyDB {

    private static final int PAGE_POOL_SIZE = 10;
    public static final int PAGE_SIZE = 1024;
    public static final int BUCKET_COUNT = 4;

    private final LinkedList<Page> pages;
    private final RandomAccessFile file;
    private Map<String, Table> tables;

    public MyDB() throws IOException {
        pages = new LinkedList<>();
        Path path = Path.of("my.db");
        System.out.println(path.toAbsolutePath());
        boolean fileExists = Files.exists(path);
        file = new RandomAccessFile(path.toString(), "rw");

        if (!fileExists) initialize();
        loadTables();
    }

    private void loadTables() throws IOException {
        tables = new HashMap<>();
        loadTablePage(0);
        Table directory = tables.get("tables");
        for (int i = 1; i < directory.getLength(); i++) {
            loadTablePage(i);
        }
    }

    private void loadTablePage(int n) throws IOException {
        Page directory = getPage(n);
        Map<String, String> values = directory.getValues();
        for (String key : values.keySet()) {
            String line = values.get(key);
            Scanner s = new Scanner(line);
            int offset = s.nextInt();
            int length = s.nextInt();
            Table t = new Table(this, key, offset, length);
            tables.put(key, t);
        }
    }

    private void initialize() throws IOException {
        System.out.println("Initializing");
        Page tables = new Page(file, 0, false);
        pages.addFirst(tables);
        tables.getValues().put("tables", "0 1");
        tables.writeValues();
    }

    public Page getPage(int n) throws IOException {
        if (pages.contains(n)) {
            return pages.get(n);
        }
        Page p = new Page(file, n);
        pages.addLast(p);
        if (pages.size() > PAGE_POOL_SIZE) {
            pages.removeFirst();
        }
        return p;
    }

    public void drop() throws IOException {
        initialize();
    }

    public Table createTable(String name) throws IOException {
        Table directory = getTable("tables");
        int length = 1;
        // TODO: fix offset
        int offset = 1;
        String value = offset + " " + length;
        directory.put(name, value);
        Table table = new Table(this, name, offset, length);
        tables.put(name, table);
        return table;
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

}
