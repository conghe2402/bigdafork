import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.parse.ParseDriver.*;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.velocity.runtime.parser.node.ASTBlock;

public class Test {
    public static void main(String[] args) throws Exception {

        String command =
                "insert overwrite table `test`.`customer_kpi` \n" +
                "SELECT\n" +
                "  base.datekey,\n" +
                "  base.clienttype,\n" +
                "  count(distinct base.userid) buyer_count\n" +
                        "FROM\n" +
                "(\n" +
                "  SELECT\n" +
                "    p.datekey datekey,\n" +
                "    p.userid userid,\n" +
                "    c.clienttype\n" +
                "  FROM\n" +
                "    `detail`.`usersequence_client` c\n" +
                "    JOIN fact.orderpayment p ON p.orderid = c.orderid\n" +
                "    JOIN users ON users.userid = p.userid\n" +
                "  WHERE p.datekey >= 20131118 and df = 'df' and ddf like '%ddf%' and dd in (select m.dd from tdsg m where dif = '2')\n" +
                ") base\n" +
                "GROUP BY base.datekey, base.clienttype";

        String command2 = "select cdf,fdf,fdg,fdf from etd a where a.dd = 202994 group by d ";
        String command3 = "alter table etd change df df2 int";
        String command4 = "create table etd (id int, uer string) partitioned by (dd string) stored as textfile";
        String command5 = "insert into table etd (id, uer) values (1, '23')";
        String command6 = "create table etd as select * from tlj ";
        String command7 = "set cd.dfdf=20";
        HiveConf conf = new HiveConf();

        conf.set("_hive.local.session.path", "D:/tmp");
        conf.set("_hive.hdfs.session.path", "D:/tmp");
        conf.set("hive.exec.local.scratchdir", "D:/tmp");
        Context c = new Context(conf);
        //SessionState.start(conf);
        //System.out.println(SessionState.get());
        //System.out.println(SessionState.get());

        ParseDriver parseDriver = new ParseDriver();
        ASTNode ast = null;
        try {
            ast = parseDriver.parse(command, c);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println();

        /*
        SemanticAnalyzer sa = new SemanticAnalyzer(conf);

        sa.analyze((ASTNode) ast.getChildren().get(0), c);
        QB qb = sa.getQB();
        System.out.println(qb.getAlias());
        //System.out.println(ast.dump());
*/


        cuseFindTok((ASTNode)ast.getChildren().get(0));

        /*
        String res = checkDDL(ast);
        System.out.println(res == null ? "ddl" : res);
        */
    }

    public static void cuseFindTok(ASTNode astn) {
        if (HiveParser.TOK_FROM == astn.getType()) {
            System.out.println("");
            if (astn.getChildCount() > 0) {
                for (Node n : astn.getChildren()) {
                    ASTNode an = (ASTNode) n;
                    cuseFindTabName(an);
                }
            }
        } else {
            if (astn.getChildCount() > 0) {
                for (Node n : astn.getChildren()) {
                    ASTNode an = (ASTNode) n;
                    cuseFindTok(an);
                }
            }
        }
    }

    public static void cuseFindTabName(ASTNode astn) {
        if (HiveParser.TOK_WHERE == astn.getType()) {
            System.out.println("!!!!!!");
            System.out.println(astn.dump());
            String[] tmp = astn.dump().trim().split("\n");
            for (int i = 1; i < tmp.length; i++) {
                if (StringUtils.isNotBlank(tmp[i])) {
                    System.out.println(tmp[i].trim());
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
                cuseFindTabName(an);
            }
        }

    }

    public static String checkDDL(ASTNode ast) {

        if (ast.getText() != null && (ast.getText().startsWith("TOK_ALTER") ||
                ast.getText().startsWith("TOK_SHOW"))) {
            return "ddl";
        } else if (HiveParser.TOK_QUERY == ast.getType()) {
            return "dml";
        }

        if (ast.getChildCount() <= 0) {
            return null;
        }
        for (Node n : ast.getChildren()) {
            ASTNode an = (ASTNode) n;
            String t = checkDDL(an);
            if (StringUtils.isNotBlank(t)) {
                return t;
            }
        }

        return null;
    }
}
