package com.opens.bigdafork.sql.parser;

import lombok.Data;

/**
 * Table ref.
 */

@Data
public class TableRef {
    private String namespace;
    private String tableName;
    private String alias;
}
