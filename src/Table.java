import java.io.IOException;

public class Table {

    private final MyDB db;
    private final String name;
    private int offset; // Starting page offset
    private int length; // Number of currently allocated pages


    public Table(MyDB d, String n, int o, int l) {
        db = d;
        name = n;
        offset = o;
        length = l;
    }

    private int bucket(String key) {
        return key.hashCode() & 0x7FFFFFFF % MyDB.BUCKET_COUNT;
    }

    public void put(String key, String value) throws IOException {
//        System.out.printf("PUT %s %s %s\n", this.name, key, value);
        int bucket = bucket(key);
        for (int i = offset + bucket; i < offset + length; i += MyDB.BUCKET_COUNT) {
            Page p = db.getPage(i);
            if (p.canPut(key, value)) {
                p.put(key, value);
                return;
            }
        }
        db.expandTable(this);
        put(key, value);
    }

    public String get(String key) throws IOException {
//        System.out.printf("GET %s %s\n", this.name, key);
        int bucket = bucket(key);
        for (int i = offset + bucket; i < offset + length; i += MyDB.BUCKET_COUNT) {
            Page p = db.getPage(i);
            String value = p.get(key);
            if (value != null) return value;
        }
        return null;
    }

    public void delete(String key) throws IOException {
//        System.out.printf("GET %s %s\n", this.name, key);
        int bucket = bucket(key);
        for (int i = offset + bucket; i < offset + length; i += MyDB.BUCKET_COUNT) {
            Page p = db.getPage(i);
            String value = p.get(key);
            if (value != null) {
                p.delete(key);
                return;
            }
        }
    }

    public int getLength() {
        return length;
    }

    public void setLength(int l) {
        length = l;
    }

    public void setOffset(int o) {
        offset = o;
    }

    public int getOffset() {
        return offset;
    }

    public String getName() {
        return name;
    }
}
