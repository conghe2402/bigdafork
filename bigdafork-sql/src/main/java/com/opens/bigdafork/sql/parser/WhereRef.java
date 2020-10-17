package com.opens.bigdafork.sql.parser;

import lombok.Data;

/**
 * Where ref.
 */
@Data
public class WhereRef {
    private String tableAlias;
    private String tableCol;
    private String relation;
    private String values;
}
