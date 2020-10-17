package com.opens.bigdafork.sql.parser.hive;

import com.opens.bigdafork.sql.parser.ISqlParser;
import com.opens.bigdafork.sql.parser.ParseResult;
import com.opens.bigdafork.sql.parser.WhereRef;
import com.opens.bigdafork.sql.parser.exception.SQLParsingException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.midi.Soundbank;
import java.io.IOException;

/**
 * HiveQLParser.
 */
public class HiveQLParser implements ISqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveQLParser.class);
    private ParseDriver parseDriver;
    private Context context;
    public HiveQLParser() throws IOException {
        HiveConf conf = new HiveConf();
        conf.set("_hive.local.session.path", "/tmp");
        conf.set("_hive.hdfs.session.path", "/tmp");
        context = new Context(conf);
        parseDriver = new ParseDriver();
        LOGGER.debug("Hive ql parser initial.");

    }

    @Override
    public ParseResult parse(String sql) throws SQLParsingException {
        ASTNode ast;
        try {
            ast = parseDriver.parse(sql, context);
        } catch (ParseException e) {
            LOGGER.error("parse error : " + e.getMessage());
            throw new SQLParsingException(e.getMessage());
        }

        LOGGER.debug("sql ast : ");
        LOGGER.debug(ast.dump());


        for (Node n : ast.getChildren()) {
            ASTNode an = (ASTNode)n;
            recurFindFromTok(an);
        }

        return null;
    }

    private void recurFindFromTok(ASTNode astn) {
        if (HiveParser.TOK_FROM == astn.getType()) {
            if (astn.getChildCount() > 0) {
                for (Node n : astn.getChildren()) {
                    ASTNode an = (ASTNode) n;
                    recurFindFromResult(an);
                }
            }
        } else {
            if (astn.getChildCount() > 0) {
                for (Node n : astn.getChildren()) {
                    ASTNode an = (ASTNode) n;
                    recurFindFromTok(an);
                }
            }
        }
    }

    /**
     * To find out source tables and relevant filter conditions.
     * @param astn
     */
    private void recurFindFromResult(ASTNode astn) {
        if (HiveParser.TOK_WHERE == astn.getType()) {
            System.out.println("!!!!!!");
            WhereRef whereRef = new WhereRef();
            System.out.println(astn.dump());
            String[] whereInfos = astn.dump().trim().split("\n");
            for (int i = 1; i < whereInfos.length; i++) {
                if (StringUtils.isNotBlank(whereInfos[i])) {
                    if (i == 1) {
                        //whereRef.setRelation(tmp[i].trim());
                    }
                    System.out.println();
                }

            }
            System.out.println("!!!!!");
        }
        else if (HiveParser.TOK_TABREF == astn.getType()) {
            System.out.println("------");
            String[] tmp = astn.dump().trim().split("\n");
            String[] tableName = new String[2];
            for (int i = 2; i < tmp.length; i++) {
                if (StringUtils.isNotBlank(tmp[i])) {
                    System.out.println(tmp[i].trim());
                }

            }
            System.out.println("------");
        }

        if (astn.getChildCount() > 0) {
            for (Node n : astn.getChildren()) {
                ASTNode an = (ASTNode) n;
                recurFindFromResult(an);
            }
        }

    }

    private String checkDDL(ASTNode astn) {

        if (astn.getText() != null && (astn.getText().startsWith("TOK_ALTER") ||
                astn.getText().startsWith("TOK_SHOW"))) {
            return "ddl";
        } else if (HiveParser.TOK_QUERY == astn.getType()) {
            return "dml";
        }

        if (astn.getChildCount() <= 0) {
            return null;
        }
        for (Node n : astn.getChildren()) {
            ASTNode an = (ASTNode) n;
            String t = checkDDL(an);
            if (StringUtils.isNotBlank(t)) {
                return t;
            }
        }

        return null;
    }
}
