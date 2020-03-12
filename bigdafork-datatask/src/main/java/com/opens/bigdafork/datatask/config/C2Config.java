package com.opens.bigdafork.datatask.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * C2Config.
 */
@Data
public class C2Config implements Cloneable {
    private int useEngine = -1;
    private int timeout = -1;
    private int standby = -1;
    private List<String> parsms;

    public List<String> getParsms() {
        if (parsms == null || parsms.size() == 0) {
            return new ArrayList<>();
        }
        List<String> newList = new ArrayList<>(parsms.size());
        for (String item : parsms) {
            newList.add(item);
        }
        return newList;
    }

    @Override
    protected C2Config clone() throws CloneNotSupportedException {
        C2Config copy = (C2Config)super.clone();
        copy.setParsms(this.getParsms());
        return copy;
    }

    public C2Config getCopy() {
        C2Config newGuy;
        try {
            newGuy = this.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            newGuy = new C2Config();
        }
        return newGuy;
    }
}
