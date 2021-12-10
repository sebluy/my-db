import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

class MyDB {
    
    private final Map<String, String> map;
    public static final Pattern PUT_PATTERN = Pattern.compile("^PUT \"([^\"]*)\" \"([^\"]*)\"$");
    public static final Pattern GET_PATTERN = Pattern.compile("^GET \"([^\"]*)\"$");
    private final Path path;
    private OutputStream out;
    
    public void loadFromFile() throws IOException {
        String file = new String(Files.readAllBytes(path));
        String[] lines = file.split("\n");
        for (String line : lines) {
            Matcher pm = PUT_PATTERN.matcher(line);
            if (pm.find()) {
                String key = pm.group(1);
                String value = pm.group(2);
                map.put(key, value);
            }
        }
        out = new BufferedOutputStream(Files.newOutputStream(path, CREATE, APPEND));
    }
    
    public MyDB() throws IOException {
        map = new HashMap<>();
        path = Paths.get("../my.db");
    }

    public void put(String key, String value) throws IOException {
        map.put(key, value);
        out.write(String.format("PUT \"%s\" \"%s\"\n", key, value).getBytes());
        out.flush();
    }

    public String get(String key) {
        return map.get(key);
    }

    public void drop() throws IOException {
        out.close();
        Files.delete(path);
        out = new BufferedOutputStream(Files.newOutputStream(path, CREATE, APPEND));
    }

}
