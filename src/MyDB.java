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
        for (Page p : pages) {
            if (p.getPageNumber() == n) return p;
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
        int offset = allocate(length);
        String value = offset + " " + length;
        directory.put(name, value);
        Table table = new Table(this, name, offset, length);
        tables.put(name, table);
        return table;
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public void expandTable(Table table) throws IOException {
        int newLength = table.getLength() * 2;
        System.out.println("Expanding " + table.getName() + " to " + newLength);
        int newOffset = allocate(newLength);
        if (newOffset != table.getOffset()) {
            copyTableData(table, newOffset);
            table.setOffset(newOffset);
        }
        table.setLength(table.getLength() * 2);
        System.out.println("Done expanding " + table.getName());
    }

    private void copyTableData(Table table, int newOffset) throws IOException {
        for (int i = 0; i < table.getLength(); i++) {
            getPage(i + table.getOffset()).copyTo(getPage(i + newOffset));
        }
    }

    private int allocate(int newLength) {
        int lastOffset = 0;
        int lastLength = 0;
        for (String key : tables.keySet()) {
            Table t = tables.get(key);
            int offset = t.getOffset();
            int difference = offset - lastOffset;
            if (difference >= newLength) return lastOffset;
            lastOffset = offset;
            lastLength = t.getLength();
        }
        return lastOffset + lastLength;
    }
}
