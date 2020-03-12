package com.opens.bigdafork.datatask.config;

import com.opens.bigdafork.common.base.config.yaml.AbstractYamlConfigLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YamlReader.
 */
public class YamlReader extends AbstractYamlConfigLoader {
    public YamlReader(String yamlPath) {
        super(yamlPath);
    }

    /**
     * load c2 map.
     * @param taskName
     * @return
     */
    public Map<String, C2Config> getAllParamsMap(String taskName) throws NullPointerException {
        Map<String, C2Config> resultMap = new HashMap<>();
        HashMap<String, Object> hashMap = (HashMap<String, Object>)this.getValue(taskName);
        for (Map.Entry<String, Object> item : hashMap.entrySet()) {
            String itemKey = item.getKey();
            C2Config c2Config = new C2Config();
            String keyName = String.format("%s.%s", taskName, itemKey);
            if ("params".equalsIgnoreCase(itemKey)) {
                //common config
                c2Config.setParsms(getParamsList(keyName));
            } else if (itemKey.trim().toLowerCase().startsWith(DataTaskConstants.SQL_INDEX_PREFIX)) {
                //in individual
                String normalKey = String.format("%s.engine.%s", keyName, "normal");
                String timeoutKey = String.format("%s.engine.%s", keyName, "timeout");
                String standbyKey = String.format("%s.engine.%s", keyName, "standby");
                if (this.getValue(normalKey) != null) {
                    c2Config.setUseEngine((int)this.getValue(normalKey));
                }
                if (this.getValue(timeoutKey) != null) {
                    c2Config.setTimeout((int)this.getValue(timeoutKey));
                }
                if (this.getValue(standbyKey) != null) {
                    c2Config.setStandby((int)this.getValue(standbyKey));
                }
                String sqlParamsKey = String.format("%s.%s", keyName, "params");
                c2Config.setParsms(getParamsList(sqlParamsKey));
            }

            resultMap.put(itemKey, c2Config);
        }
        return resultMap;
    }

    public List<String> getParamsList(String key) throws NullPointerException {
        List<String> params = new ArrayList<>();
        HashMap<String, Object> hashMap = (HashMap<String, Object>)this.getValue(key);
        for (Map.Entry<String, Object> item : hashMap.entrySet()) {
            String itemKey = item.getKey();
            String itemValue = String.valueOf(item.getValue());
            StringBuilder itemStr = new StringBuilder(itemKey);
            itemStr.append("=").append(itemValue);
            params.add(itemStr.toString());
        }
        return params;
    }

    public static void main(String[] args) {
        YamlReader reader = new YamlReader("D:\\github\\bigdafork\\bigdafork-datatask\\config\\cutom\\params.c2");

        Map<String, C2Config> map = reader.getAllParamsMap("c1\\.test");
        for (String k: map.keySet()) {
            System.out.println(k);
            System.out.println(String.format("engine : %s ; standby : %s ; timeout : %s",
                    map.get(k).getUseEngine(),
                    map.get(k).getStandby(),
                    map.get(k).getTimeout()));
            if (map.get(k).getParsms() != null) {
                for (int i = 0; i < map.get(k).getParsms().size(); i++) {
                    System.out.println("config: " + map.get(k).getParsms().get(i));
                }
            }
        }
        /*
        List<String>  commParams = reader.getParamsList("xxxTaskName.sql2.params");
        for (String a : commParams) {
            System.out.println(a);
        }

        List<String>  commParams2 = reader.getParamsList("xxxTaskName.params");
        for (String a : commParams2) {
            System.out.println(a);
        }

        List<String>  commParams3 = reader.getParamsList("xxxTaskName");
        for (String a : commParams3) {
            System.out.println(a);
        }

        System.out.println(reader.getValue("xxxTaskName.sql1.engine.normal1"));
        */
    }
}
