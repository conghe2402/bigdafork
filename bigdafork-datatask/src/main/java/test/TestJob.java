package test;

import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.works.WorkService;
import com.opens.bigdafork.datatask.works.WorkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestJob.
 */
public class TestJob extends WorkService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestJob.class);
    @Override
    public void execute(JobBean jobBean) {
        LOGGER.info("TestJob execute");
        LOGGER.info("jobBean : getUseEngine : " + jobBean.getUseEngine());
        String dml = "create table d (a int) ";
        String sql1 = "create table dd select * from dd2 where s != 5";
        String dml2 = "alter table add column a (*)";
        String sql2 = "select * from tab where b = 1";

        WorkUtils.executeUpdate(dml);
        WorkUtils.executeUpdate(sql1);
        WorkUtils.executeUpdate(dml2);
        WorkUtils.executeUpdate(sql2);

    }
}
