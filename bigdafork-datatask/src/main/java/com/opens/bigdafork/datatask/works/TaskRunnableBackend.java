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
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private DefaultServiceChain<TaskChainContext> serviceChain;
    private String userKeyTabFile = SingleContext.get().getUserKeyTabFile();
    private String userPrincipal = SingleContext.get().getUserPrincipal();
    private String envConfigPath = SingleContext.get().getEnvConfigPath();

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

    public Object runDDLTask(String ddl, int engine, RTaskType tt) throws TaskFailException {
        LOGGER.info(String.format("\nDDL TASK:\n [\n   tid:\n     %s\n   type:\n     %s" +
                        "\n   ddl sql:\n     %s\n   using engine of %s\n ]",
                SingleContext.get().getTaskId(),
                tt,
                ddl,
                engine));

        RTask runnableTask = getRunnableTaskByInfo(engine,
                tt,
                ddl, null);
        Object result = null;
        try {
            TaskResult taskResult = runSingle(runnableTask);
            if (tt == RTaskType.ddlset) {
                result = taskResult.getResultSet();
            }
        } catch (TaskFailException e) {
            //stop.
            this.notifyer.notifyStopWithoutRecord();
        }
        return result;
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
            runnableTask.free();
            if (LOGGER.isDebugEnabled()) {
                e.printStackTrace();
            }
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
            runnableTask.free();
            if (LOGGER.isDebugEnabled()) {
                e.printStackTrace();
            }
            String err = "execute fail caused by " + e.getMessage();
            LOGGER.error(String.format("execute fail caused by: \n %s \n %s",
                    runnableTask.getSql(), e.getMessage()));
            throw new TaskFailException(err);
        } catch (TimeoutException e) {
            runnableTask.free();
            String err = "execute timeout caused by execute timeout";
            LOGGER.error(String.format("execute timeout \n %s",
                    runnableTask.getSql()));
            throw new TaskFailException(err);
        }
    }

    public void close() {
        try {
            if (hiveDDLConnection != null) {
                hiveDDLConnection.close();
            }
            if (hiveConnection != null) {
                hiveConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.error(String.format("close hive connection fail, caused by ", e.getMessage()));
        }
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
            if (LOGGER.isDebugEnabled()) {
                e.printStackTrace();
            }
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
        public abstract TaskResult executeDDLSet() throws Exception;
        public abstract TaskResult executeQryRs() throws Exception;
        public abstract TaskResult executeCount() throws Exception;

        public void free() {

        }

        @Override
        public TaskResult call() throws Exception {
            TaskResult result;
            if (type == RTaskType.ddl) {
                result = executeDDL();
            } else if (type == RTaskType.ddlset) {
                result = executeDDLSet();
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
            Connection hiveConnection = getHiveDDLConnection();
            try(Statement statement = HiveOpUtils.getStatement(hiveConnection)) {
                HiveOpUtils.executeStatement(statement,
                        String.format("set hive.execution.engine=%s", engineName));
                HiveOpUtils.executeStatement(statement, this.getSql());
            }
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            return result;
        }

        protected TaskResult executeDDLSetByEngineName(String engineName) throws Exception {
            Connection hiveConnection = getHiveDDLConnection();
            Statement statement = HiveOpUtils.getStatement(hiveConnection);
            HiveOpUtils.executeStatement(statement,
                    String.format("set hive.execution.engine=%s", engineName));
            ResultSet resultset = HiveOpUtils.executeStatementQry(statement, this.getSql());

            TaskResult result = new TaskResult();
            result.setSuccess(true);
            result.setResultSet(resultset);
            return result;
        }

        protected TaskResult executeSQLByEngineName(String engineName) throws Exception {
            try (Connection hiveConnection = getHiveConnection()) {
                Statement statement = HiveOpUtils.getStatement(hiveConnection);
                loadConfigs(statement, engineName);
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
            loadConfigs(statement, engineName);
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
            loadConfigs(statement, engineName);
            HiveOpUtils.executeStatement(statement,
                    String.format("set hive.execution.engine=%s", engineName));
            ResultSet resultSet = HiveOpUtils.executeStatementQry(statement, this.getSql());
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            result.setResultSet(resultSet);
            return result;
        }

        private void loadConfigs(Statement statement, String engineName) throws Exception {
            for (String item : this.getConfigs()) {
                HiveOpUtils.executeStatement(statement, String.format("set %s", item));
                LOGGER.info(String.format("set configs on %s : %s ", engineName, item));
            }
        }
    }

    /**
     * RTaskType.
     */
    public enum RTaskType {
        ddl, ddlset, sql, set, num
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
        public TaskResult executeDDLSet() throws Exception {
            LOGGER.info("Hive On Spark DDL set execute!");
            return this.executeDDLSetByEngineName(engineName);
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
        public TaskResult executeDDLSet() throws Exception {
            LOGGER.info("Hive On MR DDL set execute!");
            return this.executeDDLSetByEngineName(engineName);
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
        private Process proc = null;
        private Thread errReader;
        private Thread normalReader;

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
            String sqls = prepareSQLWithConfigs();
            String normalMsg = submitSparkSQL(sqls);
            TaskResult result = new TaskResult();
            result.setSuccess(true);
            result.setMsg(normalMsg);
            return result;
        }

        @Override
        public TaskResult executeDDL() throws Exception {
            LOGGER.info("SPARK ddl execute!");
            TaskResult result = new TaskResult();

            Map<String, CircularFifoBuffer> resultLines = submitSparkShell(this.getSql());
            for (int i = 0; i < resultLines.size(); i++){
                if (resultLines.get(i).contains(DataTaskConstants.SPARKSQL_RESULT_SUCC)) {
                    result.setSuccess(true);
                    return result;
                } else {
                    // err msg.
                }
            }
            throw new TaskFailException("spark sql execute fail.");
        }

        @Override
        public TaskResult executeDDLSet() throws Exception {
            return null;
        }

        @Override
        public TaskResult executeQryRs() throws Exception {
            return null;
        }

        @Override
        public TaskResult executeCount() throws Exception {
            return null;
        }

        private String prepareSQLWithConfigs() {
            StringBuilder allStatements = new StringBuilder();
            if (this.getConfigs() != null) {
                for (String item : this.getConfigs()) {
                    String setItem = String.format("set %s;", item);
                    LOGGER.debug("set configs on SPARK SQL " + item);
                    allStatements.append(setItem);
                }
            }

            allStatements.append(this.getSql());
            return allStatements.toString();
        }

        private String submitSparkSQL(String sqls) throws Exception {
            Map<String, CircularFifoBuffer> resultMap = submitSparkShell(sqls);
            checkoutResult(resultMap.get("1"));
            checkoutResult(resultMap.get(DataTaskConstants.SPARKSQL_RESULT_FAIL));

            CircularFifoBuffer normalLines = resultMap.get(DataTaskConstants.SPARKSQL_RESULT_SUCC);
            if (normalLines == null) {
                throw new TaskFailException("spark sql execute fail.\n caused by script unknown err");
            }

            StringBuilder normalMsgBuilder = new StringBuilder();
            int size = normalLines.size();
            for (int i = 0; i < size; i++) {
                String nl = normalLines.remove().toString() + "\n";
                LOGGER.debug(nl);
                normalMsgBuilder.append(nl);
            }
            return normalMsgBuilder.toString();
        }

        private void checkoutResult(CircularFifoBuffer resultLines) throws Exception {
            if (resultLines != null) {
                StringBuilder failMsgBuilder = new StringBuilder();
                int size = resultLines.size();
                for (int i = 0; i < size; i++) {
                    failMsgBuilder.append(resultLines.remove().toString() + "\n");
                }
                throw new TaskFailException(String.format("spark sql execute fail.\n caused by %s",
                        failMsgBuilder.toString()));
            }
        }

        private Map<String, CircularFifoBuffer> submitSparkShell(String cmd) throws Exception {
            LOGGER.info("execute cmd");
            Map<String, CircularFifoBuffer> result = new HashMap<>();
            final CircularFifoBuffer inputLines = new CircularFifoBuffer(1000);
            final CircularFifoBuffer errLines = new CircularFifoBuffer(20);

            /*
            // spark-beeline cannot find this file in the way java invokes shell
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

            */

            String sqls = cmd;
            if (!sqls.endsWith(";")) {
                sqls.concat(";");
            }
            String scmd = String.format("/bin/sh %s %s", this.submitScript);

            try {
                proc = Runtime.getRuntime().exec(scmd,
                        new String[]{
                                String.format("ENV_PATH=%s", envConfigPath),
                                String.format("KEY_TAB_FILE=%s", userKeyTabFile),
                                String.format("USER_PRINCIPAL=%s", userPrincipal),
                                String.format("SQL_FILE=%s", sqls),
                        });
                final List<String> lastLine = new ArrayList<>(1);

                normalReader = new Thread() {
                    private BufferedReader br;
                    @Override
                    public void run() {
                        try {
                            br = new BufferedReader(
                                    new InputStreamReader(proc.getInputStream(), "UTF-8"));
                            String line;
                            while ((line = br.readLine()) != null) {
                                inputLines.add(line);
                                lastLine.clear();
                                lastLine.add(line);
                            }
                        } catch (Exception e) {
                            LOGGER.error("normal Reader exception : " + e.getMessage());
                        } finally {
                            close();
                        }
                    }

                    @Override
                    public void interrupt() {
                        close();
                        super.interrupt();
                    }

                    private void close() {
                        if (br != null) {
                            try {
                                br.close();
                            } catch (IOException e) {
                                LOGGER.error("normal Reader close err : " + e.getMessage());
                            }
                        }
                    }

                };

                errReader = new Thread() {
                    private BufferedReader br;
                    @Override
                    public void run() {
                        try {
                            br = new BufferedReader(
                                    new InputStreamReader(proc.getErrorStream(), "UTF-8"));
                            String line;
                            while ((line = br.readLine()) != null) {
                                errLines.add(line);
                            }
                        } catch (Exception e) {
                            LOGGER.error("err Reader exception : " + e.getMessage());
                        } finally {
                            close();
                        }
                    }

                    @Override
                    public void interrupt() {
                        close();
                        super.interrupt();
                    }

                    private void close() {
                        if (br != null) {
                            try {
                                br.close();
                            } catch (IOException e) {
                                LOGGER.error("normal Reader close err : " + e.getMessage());
                            }
                        }
                    }

                };

                this.normalReader.start();
                this.errReader.start();

                int runStatus = proc.waitFor();

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    LOGGER.error("weird sth");
                }

                LOGGER.info("End up with : " + lastLine.get(0));
                if ("".equals(lastLine.get(0))) {
                    result.put(DataTaskConstants.SPARKSQL_RESULT_FAIL, inputLines);
                } else {
                    result.put(lastLine.get(0), inputLines);
                }

                if (runStatus != 0) {
                    result.put("1", errLines);
                } else if ("".equals(lastLine)) {
                    errLines.add("spark sql cmd interrupt");
                    result.put("1", errLines);
                }
                LOGGER.info("runStatus : " + runStatus);
                return result;
            } finally {
                free();
            }
        }

        @Override
        public void free() {
            if (proc != null) {
                proc.destroy();
                proc = null;
                LOGGER.debug("spark sql beeline process destroyed");
            }

            if (normalReader != null && normalReader.isAlive()) {
                normalReader.interrupt();
                LOGGER.debug("spark sql beeline normalReader interrupt");
            }

            if (errReader != null && errReader.isAlive()) {
                errReader.interrupt();
                LOGGER.debug("spark sql beeline errReader interrupt");
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
