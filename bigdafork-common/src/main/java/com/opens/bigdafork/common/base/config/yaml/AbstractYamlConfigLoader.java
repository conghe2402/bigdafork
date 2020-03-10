package com.opens.bigdafork.common.base.config.yaml;

import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * AbstractYamlConfigLoader.
 */
public abstract class AbstractYamlConfigLoader {
    private String yamlPath;
    @Getter
    private Map<String, Object> properties;

    public AbstractYamlConfigLoader() {

    }

    public AbstractYamlConfigLoader(String yamlPath) {
        this.yamlPath = yamlPath;
        this.load();
    }

    public void load() {
        this.load(this.yamlPath);
    }

    public void load(String yamlFile) {
        try {
            InputStream input = new FileInputStream(yamlFile);
            Yaml yaml = new Yaml();
            properties = (Map<String, Object>) yaml.load(input);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Object getValue(String key) {
        String[] values = key.split("\\.");
        Map m = properties;
        int len = values.length - 1;
        for (int i = 0; i < len; i++) {
            if (m.containsKey(values[i])) {
                m = (Map) m.get(values[i]);
            } else {
                return null;
            }
        }
        return m.get(values[len]);
    }
}
