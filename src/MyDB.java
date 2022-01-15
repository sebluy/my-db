import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

class MyDB {

    private static final int PAGE_POOL_SIZE = 32;
    public static final int PAGE_SIZE = 1024;
    public static final int BUCKET_COUNT = 32;

    private final LinkedList<Page> pages;
    private final RandomAccessFile file;
    private final Map<String, Table> tables;

    // TODO: Use BTree
    // TODO: Support transactions
    // TODO: Multiple columns
    // TODO: Multiple indexes
    // TODO: Support parallel commands
    // TODO: Distributed?

    public MyDB() throws IOException {
        pages = new LinkedList<>();
        tables = new HashMap<>();
        Path path = Path.of("my.db");
        System.out.println(path.toAbsolutePath());
        boolean fileExists = Files.exists(path);
        file = new RandomAccessFile(path.toString(), "rw");

        if (!fileExists) initialize();
        loadTables();
    }

    private void loadTables() throws IOException {
        // TODO: variable length tables table
        Table tablesTable = new Table(this, "tables", 0, MyDB.BUCKET_COUNT);
        tables.put("tables", tablesTable);
        for (int i = tablesTable.getOffset(); i < tablesTable.getLength(); i++) {
            Page page = getPage(i);
            Map<String, String> values = page.getValues();
            for (String key : values.keySet()) {
                String line = values.get(key);
                Scanner s = new Scanner(line);
                int offset = s.nextInt();
                int length = s.nextInt();
                Table t = new Table(this, key, offset, length);
                tables.put(key, t);
            }
        }
    }

    private void initialize() throws IOException {
        System.out.println("Initializing");
        pages.clear();
        tables.clear();
        allocate(MyDB.BUCKET_COUNT);
        Table tablesTable = new Table(this, "tables", 0, MyDB.BUCKET_COUNT);
        tables.put("tables", tablesTable);
    }

    public Page getPage(int n) throws IOException {
//        System.out.println("Page: " + n);
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
        Table tablesTable = getTable("tables");
        int length = MyDB.BUCKET_COUNT;
        int offset = allocate(length);
        String value = offset + " " + length;
        tablesTable.put(name, value);
        Table table = new Table(this, name, offset, length);
        tables.put(name, table);
        return table;
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public void expandTable(Table table) throws IOException {
        // TODO: Try to allocate at the end of current block if possible. Add a reallocate(Table) method.
        int newLength = table.getLength() * 2;
        System.out.println("Expanding " + table.getName() + " to " + newLength);
        int newOffset = allocate(newLength);
        copyTableData(table, newOffset);
        table.setOffset(newOffset);
        table.setLength(table.getLength() * 2);
        System.out.println("Done expanding " + table.getName());
    }

    private void copyTableData(Table table, int newOffset) throws IOException {
        for (int i = 0; i < table.getLength(); i++) {
            getPage(i + table.getOffset()).copyTo(getPage(i + newOffset));
        }
    }

    private int allocate(int newLength) throws IOException {
        List<Table> sTables = new ArrayList<>(tables.values());
        sTables.sort(Comparator.comparingInt(Table::getOffset));
        int lastEnd = 0;
        for (Table t : sTables) {
            int start = t.getOffset();
            if (lastEnd + newLength < start) break;
            lastEnd = t.getOffset() + t.getLength();
        }
        for (int i = lastEnd; i < lastEnd + newLength; i++) {
            this.getPage(i).clear();
        }
        return lastEnd;
    }
}
