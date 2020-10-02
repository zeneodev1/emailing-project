package tech.bluemail.platform.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import tech.bluemail.platform.logging.Logger;

public class Mapper {
    public static Object getMapValue(TreeMap map, String key, Object defaultValue) {
        if (map != null && !map.isEmpty() && map.containsKey(key)) {
            Object value = map.get(key);
            if (value != null)
                return value;
        }
        return defaultValue;
    }

    public static Object getMapValue(HashMap map, String key, Object defaultValue) {
        if (map != null && !map.isEmpty() && map.containsKey(key)) {
            Object value = map.get(key);
            if (value != null)
                return value;
        }
        return defaultValue;
    }

    public static HashMap<String, String> readProperties(String filePath) {
        HashMap<String, String> results = new HashMap<>();
        Properties properties = new Properties();
        try {
            InputStream in = FileUtils.openInputStream(new File(filePath));
            properties.load(in);
            properties.stringPropertyNames().forEach(key -> {
                String value = properties.getProperty(key);
                results.put(key, value);
            });
        } catch (IOException e) {
            Logger.error(e, Mapper.class);
        }
        return results;
    }
}
