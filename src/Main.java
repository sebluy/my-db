import java.io.Console;
import java.io.IOException;
import java.util.regex.Matcher;

class Main {

    public static void main(String[] args) throws IOException {
        MyDB db = new MyDB();
        db.loadFromFile();
        runSimpleBenchmark(db, 10000);
//        runUserLoop(db);
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

        t1 = System.currentTimeMillis();
        db.loadFromFile();
        t2 = System.currentTimeMillis();
        System.out.println("Load: " + (t2 - t1) + "ms");
    }

    private static void runUserLoop(MyDB db) throws IOException {
        Console c = System.console();

        while (true) {
            String line = c.readLine(">> ");
            Matcher pm = MyDB.PUT_PATTERN.matcher(line);
            boolean found = false;
            if (pm.find()) {
                String key = pm.group(1);
                String value = pm.group(2);
                db.put(key, value);
                found = true;
            }
            Matcher gm = MyDB.GET_PATTERN.matcher(line);
            if (!found && gm.find()) {
                String key = gm.group(1);
                c.printf(db.get(key) + "\n");
                found = true;
            }
            if (!found) {
                c.printf("DOESN'T MATCH THE PATTERN\n");
            }
        }
    }

}
