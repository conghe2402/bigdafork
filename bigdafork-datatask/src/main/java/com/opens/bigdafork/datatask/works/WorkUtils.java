package com.opens.bigdafork.datatask.works;

import com.opens.bigdafork.datatask.utils.SingleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkUtils is a set of api what we can use to pass sqls to all kinds
 * of engines to execute.
 * It supports some features under the hook as below:
 * 1.If a task runs timeout or fail in default engine and we choose to set tolerance
 * configs, it the task will be restart to execute in auxiliary engine that we choose.
 * 2.To record task result every time when we try to run task using any engine.
 * 3.To submit sql statement to relevant engine that you choose according to your c1 configs.
 * 4.To load the configs that you have configured in c2 configuration file before you need to run a sql.
 */
public final class WorkUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkUtils.class);
    public static void executeUpdate(String sql) {
        if (SingleContext.get().isNewTag()) {
            LOGGER.info("new !!!!!!!!!");
            TaskManager.getInstance().submitSQLTask(sql, ApiType.executeUpdate);

        } else {
//old logic
            LOGGER.info("old !!!!!!!!!!!");
            System.out.println(sql);
        }


    }

    private WorkUtils() {}

    public static void main(String[] args) {
        char a = '\u0905';
        System.out.println(a);
    }
}
