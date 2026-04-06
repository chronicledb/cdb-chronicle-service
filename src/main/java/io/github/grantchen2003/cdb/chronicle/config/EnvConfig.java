package io.github.grantchen2003.cdb.chronicle.config;

public class EnvConfig {
    private EnvConfig() {}

    public static String get(String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable not set: " + name);
        }
        return value;
    }
}