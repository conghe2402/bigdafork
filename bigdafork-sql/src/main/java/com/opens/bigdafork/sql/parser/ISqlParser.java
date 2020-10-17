package com.opens.bigdafork.sql.parser;

import com.opens.bigdafork.sql.parser.exception.SQLParsingException;

/**
 * abstraction of sql parser.
 */
public interface ISqlParser<R extends ParseResult> {
    R parse(String sql) throws SQLParsingException;
}
