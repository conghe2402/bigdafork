package com.opens.bigdafork.sql.parser;

import lombok.Data;

import java.util.List;

/**
 * parse result.
 */

@Data
public class ParseResult {
    private boolean isDDL;
    private List<TableRef> tableRef;
    private List<WhereRef> whereRefs;
}
