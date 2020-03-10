package com.opens.bigdafork.datatask.works;

import com.opens.bigdafork.common.base.chain.simple.impl.DefaultServiceChain;
import com.opens.bigdafork.datatask.config.DataTaskConstants;
import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.exception.TaskFailException;
import com.opens.bigdafork.datatask.record.RecordBean;
import com.opens.bigdafork.datatask.record.RecordLogBean;
import com.opens.bigdafork.datatask.utils.RecordNotifyer;
import com.opens.bigdafork.datatask.utils.SingleContext;
import com.opens.bigdafork.datatask.works.arrange.*;
import com.opens.bigdafork.utils.tools.hive.manage.HiveManageUtils;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * TaskRunnableBackend.
 */
public class TaskRunnableBackend {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunnableBackend.class);
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private RecordNotifyer notifyer;
    private HiveManageUtils hiveManageUtils;
    private Configuration configuration;
    private Connection hiveConnection;
    private Connection hiveDDLConnection;
    private JobBean jobBean;
    private DefaultServiceChain<TaskChainContext, TaskChainContext, TaskChainContext> serviceChain;

    public TaskRunnableBackend() {
        notifyer = SingleContext.get().getRecordNotifyer();
        hiveManageUtils = new HiveManageUtils();
        configuration = hiveManageUtils.getEnvConfiguration();
        jobBean = SingleContext.get().getJobBean();
    }

    public RecordNotifyer getNotifyer() {
        return notifyer;
    }

    public Object runTask(SubmitTaskInfo submitTaskInfo) throws TaskFailException {
        if (submitTaskInfo.getTaskType() == null ||
                submitTaskInfo.getTaskType() == RTaskType.ddl) {
            throw new TaskFailException("RTaskType is not [sql, set, num], " +
                    "RTaskType is " + submitTaskInfo.getTaskType());
        }

        logSubmitTaskInfo(submitTaskInfo);

        RecordBean recordBean = initRecordBean(submitTaskInfo);
        RecordLogBean recordLogBean = initRecordLogBean(submitTaskInfo);

        RTask runnableTask =
                getRunnableTaskByInfo(submitTaskInfo.getUseEngine(),
                        submitTaskInfo.getTaskType(),
                        submitTaskInfo.getSql(),
                        submitTaskInfo.getConfigs());

        serviceChain = new DefaultServiceChain(new ProxyAddWorkRecord(this));
        TaskChainContext chainContext = new TaskChainContext();

        if (submitTaskInfo.getToleranceType() == 0) {
            serviceChain.addChainPart(new NormalTask(this));

        } else if (submitTaskInfo.getToleranceType() == 1) {
            serviceChain.addChainPart(new FailOverTask(this))
                        .addChainPart(new NormalTask(this));
        } else if (submitTaskInfo.getToleranceType() == 2){
            serviceChain.addChainPart(new FailoverAndTimeoutTask(this))
                        .addChainPart(new NormalTask(this));
        } else {
            LOGGER.warn("invalid tolerance type so that do nothing!");
            return null;
        }

        chainContext.put(TaskChainContext.KEY_RUN_TASK, runnableTask);
        chainContext.put(TaskChainContext.KEY_SUBMIT_TASKINFO, submitTaskInfo);
        chainContext.put(TaskChainContext.KEY_RECODE, recordBean);
        chainContext.put(TaskChainContext.KEY_RECORD_LOG, recordLogBean);
        serviceChain.init(chainContext);
        return serviceChain.run();
    }

    public void runDDLTask(String ddl, int engine) throws TaskFailException {
        LOGGER.info(String.format("\nDDL TASK:\n [\n   tid:\n     %s\n   ddl sql:" +
                        "\n     %s\n   using engine of %s\n ]",
                SingleContext.get().getTaskId(),
                ddl,
                engine));

        RTask runnableTask = getRunnableTaskByInfo(engine,
                RTaskType.ddl,
                ddl, null);
        try {
            runSingle(runnableTask);
        } catch (TaskFailException e) {
            //stop.
            this.notifyer.notifyStopWithoutRecord();
        }
    }

    /**
     * execute endpoint 1.
     * Throw a TaskFailException except result is success.
     * @param runnableTask
     * @return
     * @throws TaskFailException
     */
    public TaskResult runSingle(RTask runnableTask) throws TaskFailException {
        Future<TaskResult> taskResultFuture = executor.submit(runnableTask);

        try {
            TaskResult result = taskResultFuture.get();
            if (result.isSuccess()) {
                LOGGER.info(String.format("execute success! \n %s",
                        runnableTask.getSql()));
                return result;
            }
            throw new TaskFailException("run fail : result is not success");
        } catch (InterruptedException |
                ExecutionException e) {
            String err = "runSingle execute fail caused by " + e.getMessage();
            LOGGER.error(String.format("runSingle execute fail \n %s \n %s",
                    runnableTask.getSql(), e.getMessage()));
            throw new TaskFailException(err);
        }
    }

    /**
     * execute endpoint considering timeout.
     * Throw a TaskFailException except result is success.
     * @param runnableTask
     * @return
     * @throws TaskFailException
     */
    public TaskResult runSingleTimeout(RTask runnableTask, int timeout)
            throws TaskFailException {
        Future<TaskResult> taskResultFuture = executor.submit(runnableTask);

        try {
            TaskResult result = taskResultFuture.get(timeout, TimeUnit.SECONDS);
            if (result.isSuccess()) {
                LOGGER.info(String.format("execute success! \n %s",
                        runnableTask.getSql()));
                return result;
            }
            throw new TaskFailException("run fail : result is not success");
        } catch (InterruptedException |
                ExecutionException e) {
            String err = "execute fail caused by " + e.getMessage();
            LOGGER.error(String.format("execute fail caused by: \n %s \n %s",
                    runnableTask.getSql(), e.getMessage()));
            throw new TaskFailException(err);
        } catch (TimeoutException e) {
            String err = "execute timeout caused by execute timeout";
            LOGGER.error(String.format("execute timeout \n %s",
                    runnableTask.getSql()));
            throw new TaskFailException(err);
        }
    }

    public void close() {
        executor.shutdownNow();
    }

    private void logSubmitTaskInfo(SubmitTaskInfo submitTaskInfo) {
        StringBuilder logMsg = new StringBuilder();
        logMsg.append("\nTASK:").append("\n [")
                .append(String.format("\n   tid:\n     %s", SingleContext.get().getTaskId()))
                .append(String.format("\n   type:\n     %s", submitTaskInfo.getTaskType()))
                .append(String.format("\n   sid:\n     %s will run using engine of %s" +
                                " with tolerance mode %s.\n   sql is:\n     %s",
                        submitTaskInfo.getSqlIndex(),
                        DataTaskConstants.ENGINE_NAMES[submitTaskInfo.getUseEngine()],
                        submitTaskInfo.getToleranceType(),
                        submitTaskInfo.getSql()))
                .append("\n   configs of sql are: ");

        for (String config : submitTaskInfo.getConfigs()) {
            logMsg.append("\n     " + config);
        }
        logMsg.append("\n ]");
        LOGGER.info(logMsg.toString());
    }

    /**
     * GET NEW HIVE CONNECTION EVERY TIME BEFORE CLOSING PREVIOUS CONN THAT NOT BE CLOSED.
     * @return
     */
    private Connection getHiveConnection() throws Exception {
        boolean isNotNull = (hiveConnection != null);
        boolean isClosed;
        try {
            isClosed = isNotNull && hiveConnection.isClosed();
        } catch (SQLException e) {
            LOGGER.error("When check out if conn is closed, happened err : " + e.getMessage());
            isClosed = false;
        }

        if (isNotNull && !isClosed) {
            LOGGER.info("close a hive connection right now!");
            hiveConnection.close();
        }

        hiveConnection = null;
        hiveConnection = this.newHiveConnection();

        return hiveConnection;
    }

    private Connection getHiveDDLConnection() throws Exception {
        boolean isNull = (hiveDDLConnection == null);
        boolean isClosed;
        try {
            isClosed = !isNull && hiveDDLConnection.isClosed();
        } catch (SQLException e) {
            LOGGER.error("When check out if conn is closed, happened err : " + e.getMessage());
            isClosed = true;
        }

        if (isNull || isClosed) {
            hiveDDLConnection = null;
            hiveDDLConnection = this.newHiveConnection();
        }
        return hiveDDLConnection;
    }

    private Connection newHiveConnection() throws Exception {
        try {
            return HiveOpUtils.getConnection(configuration);
        } catch (InstantiationException | IllegalAccessException |
                ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            LOGGER.error("connection get fail! " + e.getMessage());
            throw e;
        }
    }

    public RTask getRunnableTaskByInfo(int engine,
                                       RTaskType type, String sql, List<String> configs) {
        RTask runnableTask;
        if (engine == DataTaskConstants.HIVE) {
            LOGGER.info("determine to use engine of Hive on MR");
            runnableTask = new HiveOnMRTask(type, sql, configs);
        } else if (engine == DataTaskConstants.HIVE_ON_SPARK) {
            LOGGER.info("determine to use engine of Hive on SPARK");
            runnableTask = new HiveOnSparkTask(type, sql, configs);
        } else if (engine == DataTaskConstants.SPARK_SQL) {
            LOGGER.info("determine to use engine of SPARK SQL");
            runnableTask = new SparkSQLTask(type, sql, configs, SingleContext.get().getTaskId());
        } else {
            //default is hive on MR
            LOGGER.warn("engine index is wrong or not set correctly, so we use MR engine by default");
            runnableTask = new HiveOnMRTask(type, sql, configs);
        }
        return runnableTask;
    }

    /**
     * get current record bean.
     * @param submitTaskInfo
     * @return
     */
    private RecordBean initRecordBean(SubmitTaskInfo submitTaskInfo) {
        RecordBean recordBean = new RecordBean();
        recordBean.setNeedRec(true);
        recordBean.setSchTaskId(jobBean.getJobId());
        recordBean.setTaskId(jobBean.getClassName());
        recordBean.setSchTaskFlowId(jobBean.getFlowId());
        recordBean.setSqlId(submitTaskInfo.getSqlIndex());
        recordBean.setSqlContent(submitTaskInfo.getSql());
        recordBean.setTid(String.format("%s_%s_%s", jobBean.getFlowId(),
                jobBean.getJobId(), submitTaskInfo.getSqlIndex()));
        recordBean.setTaskDesc("");
        /*
        recordBean.setNeedStop(false);
        recordBean.setStatus(1);
        recordBean.setLastCompTime();
        recordBean.setLastElapse();
        */

        return recordBean;
    }

    /**
     * get current record log bean.
     * @param submitTaskInfo
     * @return
     */
    private RecordLogBean initRecordLogBean(SubmitTaskInfo submitTaskInfo) {
        RecordLogBean recordLogBean = new RecordLogBean();
        recordLogBean.setBusinessDate(jobBean.getBusinessDate());
        recordLogBean.setSchTaskFlowId(jobBean.getFlowId());
        recordLogBean.setSchTaskId(jobBean.getJobId());
        recordLogBean.setSqlId(submitTaskInfo.getSqlIndex());
        recordLogBean.setStatus(3);
        recordLogBean.setStatusDesc("run");
        recordLogBean.setTaskId(jobBean.getClassName());
        recordLogBean.setTaskName(jobBean.getJobName());
        recordLogBean.setTid(String.format("%s_%s_%s", jobBean.getFlowId(),
                jobBean.getJobId(), submitTaskInfo.getSqlIndex()));
        return recordLogBean;

        //recordLogBean.setStartTime();
        //recordLogBean.setEndTime();
        //recordLogBean.setRid();
        //recordLogBean.setParDate();
        //recordLogBean.setEngine();
        //recordLogBean.setEngineChooseType();
        //recordLogBean.setElapse();
    }

    /**
     * type : ddl | sql | set | num
     *
     * Just do one thing : execute sql on one engine with configs.
     * Setting configs is before executing.
     * Make sure to close connection after use in the non-ddl scenario.
     * Keep the single connection alive in the ddl scenario.
     */
    public abstract class RTask implements Callable<TaskResult> {
        private RTaskType type;
        private String sql;
        private List<String> configs;
        private List<String> restoreConfigs;

        public RTask(RTaskType type, String sql, List<String> configs) {
            this.type = type;
            this.sql = sql;
            this.configs = configs;
        }

        public RTask(RTaskType type, String sql,
                     List<String> configs, List<String> restoreConfigs) {
            this(type, sql, configs);
            this.restoreConfigs = restoreConfigs;
        }

        public abstract TaskResult executeSQL() throws Exception;
        public abstract TaskResult executeDDL() throws Exception;
        public abstract TaskResult executeQryRs() throws Exception;
        public abstract TaskResult executeCount() throws Exception;

        @Override
        public TaskResult call() throws Exception {
            TaskResult result;
            if (type == RTaskType.ddl) {
                result = executeDDL();
            } else if (type == RTaskType.sql) {
                result = executeSQL();
            } else if (type == RTaskType.set) {
                result = executeQryRs();
            } else if (type == RTaskType.num) {
                result = executeCount();
            } else {
                throw new TaskFailException("task type is invalid : " + type);
            }
            return result;
        }

        public String getSql() {
            return sql;
        }

        public List<String> getConfigs() {
            return this.configs;
        }

        public RTaskType getType() {
            return this.type;
        }

        protected TaskResult executeDDLByEngineName(String engineName) throws Exception {
            try (Connection hiveConnection = getHiveDDLConnection()) {
                Statement statement = HiveOpUtils.getStatement(hiveConnection);
                HiveOpUtils.executeStatement(statement,
                        String.format("set hive.execution.engine=%s", engineName));
                HiveOpUtils.executeStatement(statement, this.getSql());
            }
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            return result;
        }

        protected TaskResult executeSQLByEngineName(String engineName) throws Exception {
            try (Connection hiveConnection = getHiveConnection()) {
                Statement statement = HiveOpUtils.getStatement(hiveConnection);
                for (String item : this.getConfigs()) {
                    HiveOpUtils.executeStatement(statement, String.format("set %s", item));
                    LOGGER.info(String.format("set configs on %s : %s ", engineName, item));
                }
                HiveOpUtils.executeStatement(statement,
                        String.format("set hive.execution.engine=%s", engineName));
                HiveOpUtils.executeStatement(statement, this.getSql());
            }

            TaskResult result = new TaskResult();
            result.setSuccess(true);
            return result;
        }

        protected TaskResult qryCountByEngineName(String engineName) throws Exception {
            Statement statement = HiveOpUtils.getStatement(getHiveConnection());
            for (String item : this.getConfigs()) {
                HiveOpUtils.executeStatement(statement, String.format("set %s", item));
                LOGGER.info(String.format("set configs on %s : %s ", engineName, item));
            }
            HiveOpUtils.executeStatement(statement,
                    String.format("set hive.execution.engine=%s", engineName));
            ResultSet resultSet = HiveOpUtils.executeStatementQry(statement, this.getSql());
            long countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getLong(1);
            }
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            result.setCountNum(countNum);
            return result;
        }

        protected TaskResult qryResultSetByEngineName(String engineName) throws Exception {
            Statement statement = HiveOpUtils.getStatement(getHiveConnection());
            for (String item : this.getConfigs()) {
                HiveOpUtils.executeStatement(statement, String.format("set %s", item));
                LOGGER.info(String.format("set configs on %s : %s ", engineName, item));
            }
            HiveOpUtils.executeStatement(statement,
                    String.format("set hive.execution.engine=%s", engineName));
            ResultSet resultSet = HiveOpUtils.executeStatementQry(statement, this.getSql());
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            result.setResultSet(resultSet);
            return result;
        }
    }

    /**
     * RTaskType.
     */
    public enum RTaskType {
        ddl, sql, set, num
    }

    /**
     * HiveOnSparkTask.
     */
    private class HiveOnSparkTask extends RTask {
        private final String engineName = "spark";
        public HiveOnSparkTask(RTaskType type, String sql, List<String> configs) {
            super(type, sql, configs);
        }

        @Override
        public TaskResult executeSQL() throws Exception {
            LOGGER.info("Hive On Spark SQL execute!");
            return this.executeSQLByEngineName(engineName);
        }

        @Override
        public TaskResult executeDDL() throws Exception {
            LOGGER.info("Hive On Spark DDL execute!");
            return this.executeDDLByEngineName(engineName);
        }

        @Override
        public TaskResult executeQryRs() throws Exception {
            LOGGER.info("Hive on Spark qry RS execute!");
            return this.qryResultSetByEngineName(engineName);
        }

        @Override
        public TaskResult executeCount() throws Exception {
            LOGGER.info("Hive on Spark qry count execute!");
            return this.qryCountByEngineName(engineName);
        }
    }

    private class HiveOnMRTask extends RTask {
        private final String engineName = "mr";

        public HiveOnMRTask(RTaskType type, String sql, List<String> configs) {
            super(type, sql, configs);
        }

        @Override
        public TaskResult executeSQL() throws Exception {
            LOGGER.info("Hive on MR SQL execute!");
            return this.executeSQLByEngineName(engineName);
        }

        @Override
        public TaskResult executeDDL() throws Exception {
            LOGGER.info("Hive on MR DDL execute!");
            return this.executeDDLByEngineName(engineName);
        }

        @Override
        public TaskResult executeQryRs() throws Exception {
            LOGGER.info("Hive on MR qry RS execute!");
            return this.qryResultSetByEngineName(engineName);
        }

        @Override
        public TaskResult executeCount() throws Exception {
            LOGGER.info("Hive on MR qry count execute!");
            return this.qryCountByEngineName(engineName);
        }

    }

    // sumbit sql to spark server by shell
    private class SparkSQLTask extends RTask {
        private String taskId;
        private String shellPath;
        private String submitScript;
        private String sqlsFilesPath;

        public SparkSQLTask(RTaskType type, String sql,
                            List<String> configs, String taskId) {
            super(type, sql, configs);
            this.taskId = taskId;
            this.shellPath = System.getProperty("use.dir") + "/ssql/";
            this.submitScript = this.shellPath + "script/submit";
            this.sqlsFilesPath = this.shellPath + "sql/";
        }

        @Override
        public TaskResult executeSQL() throws Exception {
            LOGGER.info("spark sql execute!");
            StringBuilder allStatements = new StringBuilder();
            if (this.getConfigs() != null) {
                for (String item : this.getConfigs()) {
                    String setItem = String.format("set %s;", item);
                    LOGGER.info("set configs on SPARK SQL! " + item);
                    allStatements.append(setItem);
                }
            }
            allStatements.append(this.getSql());
            TaskResult result = new TaskResult();
            List<String> resultLines = submitSparkShell(this.getSql());
            for (int i = 0; i < resultLines.size(); i++){
                if (resultLines.get(i).contains("complete123")) {
                    result.setSuccess(true);
                    return result;
                } else {
                    // err msg.
                }
            }
            throw new TaskFailException("spark sql execute fail.");
        }

        @Override
        public TaskResult executeDDL() throws Exception {
            LOGGER.info("SPARK ddl execute!");
            TaskResult result = new TaskResult();

            List<String> resultLines = submitSparkShell(this.getSql());
            for (int i = 0; i < resultLines.size(); i++){
                if (resultLines.get(i).contains("complete123")) {
                    result.setSuccess(true);
                    return result;
                } else {
                    // err msg.
                }
            }
            throw new TaskFailException("spark sql execute fail.");
        }

        @Override
        public TaskResult executeQryRs() throws Exception {
            return null;
        }

        @Override
        public TaskResult executeCount() throws Exception {
            return null;
        }

        private List<String> submitSparkShell(String cmd) throws Exception {
            LOGGER.info("execute cmd");
            List<String> resultLines = new ArrayList<>();
            String sqlsFile = this.sqlsFilesPath + this.taskId;
            File tempFile = new File(sqlsFile);
            if (tempFile.exists()) {
                tempFile.delete();
            }
            FileWriter wf = new FileWriter(tempFile);
            if (!cmd.endsWith(";")) {
                wf.write(cmd + ";");
            } else {
                wf.write(cmd);
            }

            wf.close();

            String scmd = String.format("sh %s %s", this.submitScript, sqlsFile);
            Process proc = null;
            try {
                proc = Runtime.getRuntime().exec(scmd);
                InputStream stdrr = proc.getInputStream();
                InputStreamReader isr = new InputStreamReader(stdrr, "UTF-8");
                BufferedReader br = new BufferedReader(isr);
                String line = br.readLine();
                while(line != null) {
                    LOGGER.info(line);
                    resultLines.add(line);
                    line = br.readLine();
                }
                return resultLines;
            } finally {
                if (proc != null) {
                    proc.destroy();
                }
            }
        }
    }

    /**
     * TaskResult.
     */
    @Data
    public class TaskResult {
        private boolean success;
        private String msg;

        //ResultSet
        private ResultSet resultSet;

        //countNum
        private long countNum;
    }

    /**
     * SubmitTaskInfo.
     */
    @Data
    public class SubmitTaskInfo {
        private String sqlIndex;
        private int useEngine;
        private int standbyEngine;
        private int timeout;
        private String sql;
        private int toleranceType; // 0:no;1:fail;2:timeout and fail;
        private List<String> configs;
        // TODO: 2020/2/18 configs for standby
        private List<String> standByConfigs;
        private boolean isDDL; //1:yes;0:no
        private RTaskType taskType;
    }
}
