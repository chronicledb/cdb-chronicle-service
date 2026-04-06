package io.github.grantchen2003.cdb.chronicle.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvConfigTest {

    @Test
    void testGet_returnsValueWhenEnvVarIsSet() {
        final String value = EnvConfig.get("PATH");
        assertNotNull(value);
        assertFalse(value.isBlank());
    }

    @Test
    void testGet_throwsWhenEnvVarIsMissing() {
        final IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                EnvConfig.get("CHRONICLE_DEFINITELY_NOT_A_REAL_ENV_VAR")
        );
        assertTrue(ex.getMessage().contains("CHRONICLE_DEFINITELY_NOT_A_REAL_ENV_VAR"));
    }
}