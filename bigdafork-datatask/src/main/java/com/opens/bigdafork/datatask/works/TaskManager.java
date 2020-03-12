package com.opens.bigdafork.datatask.works;

import com.opens.bigdafork.datatask.config.*;
import com.opens.bigdafork.datatask.record.WorkRecorder;
import com.opens.bigdafork.datatask.utils.FileUtils;
import com.opens.bigdafork.datatask.utils.RecordNotifyer;
import com.opens.bigdafork.datatask.utils.SingleContext;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.RTaskType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TaskManager.
 * 
 */
// TODO: 2020/2/18 check out if get started with last success progress when restart to run.
public final class TaskManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);
    private static final String[] ENGINES_NAMES = {"",
        DataTaskConstants.HIVE_ON_MR_NAME,
        DataTaskConstants.HIVE_ON_SPARK_NAME,
        DataTaskConstants.SPARK_SQL_NAME};
    private static TaskManager instance;

    private TaskRunnableBackend taskRunnableBackend = new TaskRunnableBackend();
    private String c2ConfigPath;
    private JobBean jobBean;
    private TaskPropertiesConfig c1Config;
    private TaskPropertiesConfig c2Config;
    private RecordNotifyer notifyer;
    private WorkRecorder workRecorder;
    private String taskId;
    private C2Config c2ItemsForTask;
    private Map<String, C2Config> c2ItemsForEachSQL = new HashMap<>();
    private int executeCount = 1;

    private int defaultEngineOfTask;
    private int defaultTimeoutOfTask;
    private int defaultStandbyEngineOfTask;
    private int defaultDMLEngineOfTask;

    private int webUseEngine;
    private int webUseRes;
    private int webStandbyEngine;
    private int webTimeout;
    private List<String> collectSetCmdOfOneSql = new ArrayList<>();

    private TaskManager() {
        c1Config = SingleContext.get().getC1Config();
        c2ConfigPath = SingleContext.get().getC2CofigPath();
        jobBean = SingleContext.get().getJobBean();
        taskId = SingleContext.get().getTaskId();
        notifyer = SingleContext.get().getRecordNotifyer();
        workRecorder = SingleContext.get().getWorkRecorder();

        //load c1 of task
        defaultDMLEngineOfTask = getDDLEngine();
        defaultEngineOfTask = getDefaultEngine();

        webUseEngine = jobBean.getUseEngine();
        if (webUseEngine == 0
                || !isLegalEngine(webUseEngine)) {
            LOGGER.warn(String.format("JobBean`s useEngine is default or invalid, " +
                    "and use c1 default engine"));
            //specify default engine through frontend
            webUseEngine = defaultEngineOfTask;
        }

        webUseRes = jobBean.getUseResLevel();
        if (isLegalEngine(jobBean.getToleranceEngine())) {
            webStandbyEngine = jobBean.getToleranceEngine();
        } else {
            webStandbyEngine = 0;
        }

        webTimeout = jobBean.getTimeout();
        loadC2OfTask(webUseEngine, webUseRes, webStandbyEngine, webTimeout);
    }

    public static TaskManager getInstance() {
        if (instance == null) {
            instance = new TaskManager();
        }
        return instance;
    }

    /**
     * submit task.
     */
    public Object submitSQLTask(String sql, ApiType apiType) {
        if (sql == null || sql.trim().equals("")) {
            LOGGER.warn("sql is null or empty! just return.");
            return null;
        }

        //set cmd
        if (sql.trim().startsWith("set ")) {
            String[] arr = sql.split(";");
            for (String item : arr) {
                if (item != null && item.trim().startsWith("set ")) {
                    item = item.substring(4);
                    this.collectSetCmdOfOneSql.add(item);
                }
            }
            return null;
        }

        if (isDDL(sql)) {
            //submit to ddl engine to exe
            taskRunnableBackend.runDDLTask(sql, defaultDMLEngineOfTask);
            return null;
        }

        // submit
        return this.submitSQL(sql, this.webUseEngine,
                this.webStandbyEngine, this.webTimeout, apiType);
    }

    public void nextSQL() {
        LOGGER.info("next sql invoke, current index " + this.executeCount);
        this.executeCount++;
    }

    public int getCurSQLIndex() {
        return this.executeCount;
    }

    public void close() {
        LOGGER.info("close all of services for free resource.");
        this.taskRunnableBackend.close();
    }

    /**
     * Loading C2 of task initially.
     * @param useEngine
     * @param useResLevel
     * @param standbyEngine
     * @param timeout
     */
    private void loadC2OfTask(int useEngine, int useResLevel,
                              int standbyEngine, int timeout) {
        if (useResLevel == DataTaskConstants.C2_RES_LEVEL_DEFAULT &&
                (hasCustomC2Config() || hasNasCustomC2Config())) {
            //try to load task c2 config if exists and resourceLevel is default.
            YamlReader yamlReader = new YamlReader(this.c2ConfigPath);
            try {
                //key: sql1 ; value ： items
                c2ItemsForEachSQL = yamlReader.getAllParamsMap(jobBean.getClassName());
            } catch (NullPointerException e) {
                LOGGER.error(String.format("self-defined c2 config is invalid. \r\n %s"), c2ConfigPath);
                loadPresetC2Config(useEngine, useResLevel, standbyEngine, timeout);
            }
        } else {
            loadPresetC2Config(useEngine, useResLevel, standbyEngine, timeout);
        }
    }

    private void loadPresetC2Config(int useEngine, int useResLevel,
                                    int standbyEngine, int timeout) {
        //try to load task default c2 config according to resource level.
        String c2ConfigFile = getC2CommonConfig(useEngine, useResLevel);
        c2Config = new TaskPropertiesConfig(c2ConfigFile, workRecorder);
        C2Config c2ConfigObj = new C2Config();
        c2ConfigObj.setUseEngine(useEngine);
        c2ConfigObj.setStandby(standbyEngine);
        c2ConfigObj.setTimeout(timeout);
        c2ConfigObj.setParsms(c2Config.getAllConfigList());
        c2ItemsForTask = c2ConfigObj;
        if (c2ItemsForTask.getParsms() == null
                || c2ItemsForTask.getParsms().isEmpty()) {
            LOGGER.warn(String.format("Res file is not found or is empty!!! \r\n %s",
                    c2ConfigFile));
        }
    }

    private String getC2CommonConfig(int useEngine, int useResLvl) {
        c2ConfigPath = SingleContext.get().getC2CofigPath();
        StringBuilder filePath = new StringBuilder(c2ConfigPath);
        if (!filePath.toString().endsWith("/")) {
            filePath.append("/");
        }
        //root/normal_c2/hiveonmr/params0.c2 :
        filePath.append("normal_c2")
                .append("/")
                .append(getEngineName(useEngine))
                .append("/params")
                .append(useResLvl)
                .append(".c2");
        LOGGER.info(String.format("try to load common c2 config. \r\n %s", filePath.toString()));
        return filePath.toString();
    }

    private String getEngineName(int engine) {
        if (isLegalEngine(engine)) {
            return ENGINES_NAMES[engine];
        } else {
            return ENGINES_NAMES[1];
        }
    }

    private boolean isLegalEngine(int engine) {
        return 1 <= engine && engine <= 3;
    }
    private boolean isLegalResLvl(int resLvl) {
        return 0 >= resLvl && resLvl <= 4;
    }

    // TODO: 2020/3/10  
    private boolean hasNasCustomC2Config() {
        String nasC2Dir = "";

        if (StringUtils.isBlank(nasC2Dir)) {
            LOGGER.debug("get the business job script path fail");
            return false;
        }


        File customC2File = new File(c2ConfigPath);
        //self-defined params.c2 is file and exists.
        //return customC2File.isFile() && customC2File.exists();
        return false;
    }

    private boolean hasCustomC2Config() {
        String clazz = jobBean.getClassName();
        String classFilePath = "";
        try {
            classFilePath = FileUtils.getPathFromClass(Class.forName(clazz));
        } catch (ClassNotFoundException | IOException e) {
            LOGGER.error("custom c2 config load fail, cause " + e.getMessage());
            e.printStackTrace();
        }

        if (StringUtils.isBlank(classFilePath)) {
            LOGGER.debug("get the business job script path fail");
            return false;
        }

        classFilePath = classFilePath.substring(0, classFilePath.lastIndexOf("\\"));
        c2ConfigPath = classFilePath + "\\params.c2";
        File customC2File = new File(c2ConfigPath);
        //self-defined params.c2 is file and exists.
        return customC2File.isFile() && customC2File.exists();
    }

    /**
     * @// TODO: 2020/2/12 ANT parse
     * @param sql
     * @return
     */
    private boolean isDDL(String sql) {
        String sqlTemp = " " + sql.trim().toLowerCase() + " ";
        if (sqlTemp.contains(" select ") ||
                sqlTemp.contains(" delete ") ||
                sqlTemp.contains(" insert ")) {
            return false;
        }
        return true;
    }

    /**
     * 1:hive ; 2:hive on spark ; 3:sparkSQL.
     * @return
     */
    private int getDefaultEngine() {
        int engine = DataTaskConstants.HIVE;
        boolean[] enginesEnable = {false, false, false, false};

        boolean hasEngineOpened = false;
        if ("1".equals(c1Config.getConfig(DataTaskConstants.KEY_ENGINE_DEFUALT_HIVE))) {
            enginesEnable[DataTaskConstants.HIVE]= true;
            hasEngineOpened = true;
        }
        if ("1".equals(c1Config.getConfig(DataTaskConstants.KEY_ENGINE_DEFUALT_HIVEONSPARK))) {
            enginesEnable[DataTaskConstants.HIVE_ON_SPARK] = true;
            hasEngineOpened = true;
        }
        if ("1".equals(c1Config.getConfig(DataTaskConstants.KEY_ENGINE_DEFUALT_SPARKSQL))) {
            enginesEnable[DataTaskConstants.SPARK_SQL] = true;
            hasEngineOpened = true;
        }

        //default engine
        if (!hasEngineOpened) {
            enginesEnable[DataTaskConstants.HIVE]= true;
        }

        String prios = c1Config.getConfig(DataTaskConstants.KEY_ENGINE_DEFUALT_PRIORITY,
                DataTaskConstants.DEFAULT_ENGINE_PRIOS);
        int[] priorities = checkPrios(prios);
        for (int i = 0; i < priorities.length; i++) {
            if (enginesEnable[priorities[i]]) {
                engine = priorities[i];
                break;
            }
        }

        return engine;
    }

    private int getDDLEngine() {
        int ddlEngine = c1Config.getConfig(DataTaskConstants.KEY_SQL_DDL_ENGINE,
                DataTaskConstants.HIVE);
        if (ddlEngine < 1 || ddlEngine > 3) {
            ddlEngine = DataTaskConstants.HIVE;
        }
        return ddlEngine;
    }

    private int[] checkPrios(String priosStr) {
        int[] prios = {DataTaskConstants.HIVE,
            DataTaskConstants.HIVE_ON_SPARK,
            DataTaskConstants.SPARK_SQL};
        try {
            String[] priosArr = priosStr.split(",");
            for (int i = 0; i < priosArr.length; i++) {
                int prio = Integer.parseInt(priosArr[i]);
                if (prio < 1 || prio > prios.length) {
                    LOGGER.warn(DataTaskConstants.KEY_ENGINE_DEFUALT_PRIORITY + " wrong! prio use default engine hive");
                    prio = DataTaskConstants.HIVE;
                }
                prios[i] = prio;
            }
        } catch (Exception e) {
            LOGGER.warn(DataTaskConstants.KEY_ENGINE_DEFUALT_PRIORITY + " wrong!");
        }
        return prios;
    }

    // just submit in sequence line , rather than parallel
    private Object submitSQL(String sql, int ue, int se,
                             int timeout, ApiType apiType) {
        TaskRunnableBackend.SubmitTaskInfo submitTaskInfo
                = taskRunnableBackend.new SubmitTaskInfo();
        submitTaskInfo.setTimeout(timeout);
        submitTaskInfo.setSql(sql);
        submitTaskInfo.setStandbyEngine(se);
        submitTaskInfo.setUseEngine(ue);
        submitTaskInfo.setTaskType(this.setTaskTypeByApi(apiType));

        //check out if there are custom configuration belong to the sql.
        String sqlIndex = DataTaskConstants.SQL_INDEX_PREFIX + this.executeCount;
        // once enter here , next sql
        this.nextSQL();

        submitTaskInfo.setSqlIndex(sqlIndex);
        C2Config configsOfSql;
        if (this.c2ItemsForEachSQL.containsKey(sqlIndex)) {
            LOGGER.info(String.format("use c2 custom config for %s", sqlIndex));
            configsOfSql = this.c2ItemsForEachSQL.get(sqlIndex);
            //get custom c2 engine config
            if (isLegalEngine(configsOfSql.getUseEngine())) {
                submitTaskInfo.setUseEngine(configsOfSql.getUseEngine());
            }
            submitTaskInfo.setStandbyEngine(configsOfSql.getStandby());
            submitTaskInfo.setTimeout(configsOfSql.getTimeout());
        } else {
            LOGGER.info(String.format("use c2 preset config for %s", sqlIndex));
            configsOfSql = c2ItemsForTask;
        }
        submitTaskInfo.setConfigs(configsOfSql.getParsms());

        //pass param by coding.
        if (this.collectSetCmdOfOneSql.size() > 0) {
            for (String item : this.collectSetCmdOfOneSql) {
                submitTaskInfo.getConfigs().add(item);
            }
            //clear
            this.collectSetCmdOfOneSql.clear();
        }

        // check out tolerance mode.
        if (isLegalEngine(submitTaskInfo.getStandbyEngine())) {
            submitTaskInfo.setToleranceType(1);
            if (submitTaskInfo.getTimeout() > 0) {
                submitTaskInfo.setToleranceType(2);
            }
        } else {
            submitTaskInfo.setToleranceType(0);
        }

        return taskRunnableBackend.runTask(submitTaskInfo);
    }

    private RTaskType setTaskTypeByApi(ApiType apiType) {
        if (apiType == ApiType.executeQueryNum) {
            return TaskRunnableBackend.RTaskType.num;
        } else if (apiType == ApiType.executeQueryRs) {
            return TaskRunnableBackend.RTaskType.set;
        } else if (apiType == ApiType.executeUpdate) {
            return TaskRunnableBackend.RTaskType.sql;
        } else if (apiType == ApiType.executeDDL) {
            return TaskRunnableBackend.RTaskType.ddl;
        } else {
            return null;
        }
    }
}
