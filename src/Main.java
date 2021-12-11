import java.io.Console;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Main {

    private static final Pattern PUT_PATTERN = Pattern.compile("^PUT \"([^\"]*)\" \"([^\"]*)\"$");
    private static final Pattern GET_PATTERN = Pattern.compile("^GET \"([^\"]*)\"$");
    private static final Pattern DELETE_PATTERN = Pattern.compile("^DELETE \"([^\"]*)\"$");

    // TODO: support growing pages, and flushing pages from cache.

    public static void main(String[] args) throws IOException {
        MyDB db = new MyDB();
//        runSimpleBenchmark(db, 100);
        runUserLoop(db);
    }

    private static void runSimpleBenchmark(MyDB db, int size) throws IOException {
        db.drop();

        long t1, t2;
        t1 = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            String s = String.valueOf(i);
            db.put(s, s);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Write: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            String s = String.valueOf(i);
            db.get(s);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Read: " + (t2 - t1) + "ms");
    }

    private static void runUserLoop(MyDB db) throws IOException {
        Console c = System.console();

        while (true) {
            String line = c.readLine(">> ");
            Matcher pm = PUT_PATTERN.matcher(line);
            boolean found = false;
            if (pm.find()) {
                String key = pm.group(1);
                String value = pm.group(2);
                db.put(key, value);
                found = true;
            }
            Matcher gm = GET_PATTERN.matcher(line);
            if (!found && gm.find()) {
                String key = gm.group(1);
                c.printf(db.get(key) + "\n");
                found = true;
            }
            Matcher dm = DELETE_PATTERN.matcher(line);
            if (!found && dm.find()) {
                String key = dm.group(1);
                db.delete(key);
                found = true;
            }
            if (!found) {
                c.printf("DOESN'T MATCH THE PATTERN\n");
            }
        }
    }

}
