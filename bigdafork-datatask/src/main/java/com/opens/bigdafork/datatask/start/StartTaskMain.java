package com.opens.bigdafork.datatask.start;

import com.opens.bigdafork.datatask.config.DataTaskConstants;
import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.config.TaskPropertiesConfig;
import com.opens.bigdafork.datatask.execute.JobService;
import com.opens.bigdafork.datatask.record.WorkRecorder;
import com.opens.bigdafork.datatask.utils.RecordNotifyer;
import com.opens.bigdafork.datatask.utils.SingleContext;
import com.opens.bigdafork.datatask.works.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * starter.
 * if use this, master switch is on by d.
 */
public final class StartTaskMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartTaskMain.class);

    static {
        //load sth
    }

    public static void main(String[] args) {
        LOGGER.info("[[NEW]] task start... ");
        /*
            for test
         */
        args = new String[]{"987000000",
            "1",
            "test fro ",
            "test.TestJob",
            "20180901",
            "0", // 任务状态
            "1", // 总控
            "0", // 0:默认; 1：hiveOnMr; 2: hiveOnSpark; 3: sparkSQL
            "0", // 备 用引擎，0:无；1：hiveOnMr; 2: hiveOnSpark; 3: sparkSQL
            "0", //
            "0", // timeout
            "Na", //
            "1", // task params1
            "dd", // task params2
            "cc", // task params3
        };
        //args
        JobBean jobBean = argsParse(args);

        //load env config
        TaskPropertiesConfig envConfig = new TaskPropertiesConfig(DataTaskConstants.ENV_CONFIG);
        String c1Path = envConfig.getConfig(DataTaskConstants.KEY_CONFIG_C1_PATH);
        String c2Path = envConfig.getConfig(DataTaskConstants.KEY_CONFIG_C2_PATH);
        String recordPath = envConfig.getConfig(DataTaskConstants.KEY_RECORD_DIR);
        String nasRootPath = envConfig.getConfig(DataTaskConstants.KEY_NAS_CONFIG_PATH);
        String userKeyTabFile = envConfig.getConfig(DataTaskConstants.USERNAME_CLIENT_KEYTAB_FILE);
        String userPrincipal = envConfig.getConfig(DataTaskConstants.USERNAME_CLIENT_KERBEROS_PRINCIPAL);
        String envConfigPath = envConfig.getConfig(DataTaskConstants.KEY_ENV_CONFIG_PATH);

        WorkRecorder workRecorder = new WorkRecorder(jobBean, recordPath);
        RecordNotifyer recordNotifyer = new RecordNotifyer(workRecorder);
        String c1File = c1Path + "/params.c1";
        TaskPropertiesConfig c1Config = new TaskPropertiesConfig(c1File, workRecorder);

        //ms is open and do not choose default as normal engine.
        if ("1".equals(jobBean.getMasterSwitch())) {
            SingleContext.get().switchNew();
        }
        SingleContext.get().setC1Config(c1Config);
        SingleContext.get().setJobBean(jobBean);
        SingleContext.get().setTaskId(jobBean.getClassName());
        SingleContext.get().setWorkRecorder(workRecorder);
        SingleContext.get().setRecordNotifyer(recordNotifyer);
        SingleContext.get().setC2CofigPath(c2Path);
        SingleContext.get().setNasRootPath(nasRootPath);
        String taskName = getTaskName(jobBean.getClassName());
        SingleContext.get().setTaskName(nasRootPath);


        //initialize work object and execute
        try {
            JobService.execute(jobBean);
        } catch (Exception e) {
            LOGGER.error("Un expected exception : " + e.getMessage());
            e.printStackTrace();
        }

        //make sure all of tasks are done completely and synchronously one by one.
        TaskManager.getInstance().close();

        LOGGER.info("[[NEW]] task complete!");
    }

    private static JobBean argsParse(String[] args){
        LOGGER.info("[[NEW]] argsParse");
        JobBean jobBean = new JobBean();

        //common
        int i = 0;
        String flowId = args[i++]; //0
        String jobId = args[i++]; //1
        String jobName = args[i++]; //2
        String classPath = args[i++]; //3
        String businessDate = args[i++]; //4
        String jobStatus = args[i++]; //5
        String masterSwitch = args[i++]; //6
        String useEngine = args[i++]; //7
        String toleranceEngine = args[i++]; //8
        String useResLevel = args[i++]; //9
        String timeout = args[i++]; //10
        jobBean.setFlowId(flowId);
        jobBean.setJobId(jobId);
        jobBean.setJobName(jobName);
        jobBean.setMasterSwitch(masterSwitch);
        jobBean.setUseEngine(Integer.parseInt(useEngine));
        jobBean.setUseResLevel(Integer.parseInt(useResLevel));
        jobBean.setTimeout(Integer.parseInt(timeout));
        jobBean.setJobStatus(jobStatus);
        jobBean.setToleranceEngine(Integer.parseInt(toleranceEngine));
        jobBean.setClassName(classPath);
        jobBean.setBusinessDate(businessDate);

        //task params
        if (args.length > i + 1) {
            String[] params = new String[args.length - (i + 1)];
            for (int k = 0; k < params.length; k++) {
                params[k] = args[i++];
            }
            //
            jobBean.setTaskParams(params);
        }
        return jobBean;
    }

    private static String getTaskName(String className) {
        if (className.indexOf('.') > 0) {
            return className.substring(className.lastIndexOf('.') + 1);
        } else {
            return className;
        }
    }

    private StartTaskMain(){}
}
