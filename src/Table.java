import java.io.IOException;

public class Table {

    private final MyDB db;
    private final String name;
    private int offset;
    private int length;


    public Table(MyDB d, String n, int o, int l) {
        db = d;
        name = n;
        offset = o;
        length = l;
    }

    public void put(String key, String value) throws IOException {
        for (int i = 0; i < length; i++) {
            Page p = db.getPage(offset + (key.hashCode() & 0x7FFFFFFF) % MyDB.BUCKET_COUNT);
            if (p.canPut(key, value)) {
                p.put(key, value);
                return;
            }
        }
        db.expandTable(this);
        put(key, value);
    }

    public String get(String key) throws IOException {
        for (int i = 0; i < length; i++) {
            Page p = db.getPage(offset + (key.hashCode() % MyDB.BUCKET_COUNT));
            String value = p.get(key);
            if (value != null) return value;
        }
        return null;
    }

    public void delete(String key) throws IOException {
        for (int i = 0; i < length; i++) {
            Page p = db.getPage(offset + (key.hashCode() % MyDB.BUCKET_COUNT));
            String value = p.get(key);
            if (value != null) p.delete(key);
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
