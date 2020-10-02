package tech.bluemail.platform.registry;

import java.util.HashMap;

public class Packager {
    private HashMap<String, Object> registry;

    private static Packager instance;

    public static Packager getInstance() {
        if (instance == null)
            instance = new Packager();
        return instance;
    }

    private Packager() {
        this.registry = new HashMap<>();
    }

    public HashMap<String, Object> getRegistry() {
        return this.registry;
    }

    public void setRegistry(HashMap<String, Object> registry) {
        this.registry = registry;
    }
}
