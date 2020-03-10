package com.opens.bigdafork.datatask.config;

import lombok.Data;

import java.util.List;

/**
 * C2Config.
 */
@Data
public class C2Config {
    private int useEngine;
    private int timeout;
    private int standby;
    private List<String> parsms;
}
