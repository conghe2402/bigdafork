package com.opens.bigdafork.datatask.record;

import com.opens.bigdafork.common.base.observe.AbstractBasicObserver;
import com.opens.bigdafork.common.base.observe.NotifyEvent;
import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * WorkRecorder.
 * 1.To get record info.
 * 2.transform info format.
 * 2.write record into file.
 */
public class WorkRecorder extends AbstractBasicObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkRecorder.class);
    private JobBean jobBean;
    private String recordDir;
    private String recordLogDir;
    private String recordDefDir;

    public WorkRecorder(JobBean jobBean, String recordDir) {
        this.jobBean = jobBean;
        this.recordDir = recordDir;
        if (!recordDir.endsWith("/") && !recordDir.endsWith("\\")) {
            this.recordDir = this.recordDir + "/";
        }

        this.recordLogDir = this.recordDir.concat("task_exe_record/");
        this.recordDefDir = this.recordDir.concat("task_sql_def/");
    }

    @Override
    public void receive(NotifyEvent obj) {
        WorkRecordMsg recordMsg = (WorkRecordMsg)obj.getPayload();
        // just only for reading config
        if (recordMsg != null && recordMsg.getConfigStatus() != null) {
            if (!recordMsg.getConfigStatus()) {
                LOGGER.error(
                        String.format("[WorkRecorder] config of work task load " +
                                "fail, cause %s", recordMsg.getConfigStatusMsg()));
            }
            return;
        }

        //log first
        if (recordMsg.getRecordLogBean() != null) {
            updateRecordLog(recordMsg.getRecordLogBean());
        }

        //record
        if (recordMsg.getRecordBean() != null
                && recordMsg.getRecordBean().isNeedRec()) {
            updateRecord(recordMsg.getRecordBean());
        }

    }

    private void updateRecord(RecordBean recordBean) {
        //recordBean.setLastCompTime();
        LOGGER.debug("[WorkRecorder] do Record !");

        if (recordBean == null) {
            LOGGER.warn("record fail cause recordBean is null");
            return;
        }

        //transer msg
        String line = recordBean.toString();
        String sqlIndex = recordBean.getSqlId();
        String sqlContent = recordBean.getSqlContent();
        String taskId = recordBean.getTaskId();

        StringBuilder pathBuilder = new StringBuilder(recordDefDir);
        pathBuilder.append("java/").append(taskId).append("/");
        String dir = pathBuilder.toString();
        File dirFile = new File(dir);
        dirFile.mkdirs();

        //write ing
        pathBuilder.append("task_sql_def."+sqlIndex+".record.ing");
        File ingFile = new File(pathBuilder.toString());
        boolean finish = writeLine(ingFile, line);

        //write sql
        StringBuilder sqlFilePathBuilder = new StringBuilder(dir);
        sqlFilePathBuilder.append(sqlIndex).append(".record");
        File sqlFile = new File(sqlFilePathBuilder.toString());
        writeLine(sqlFile, sqlContent);

        //rename ing to complete file
        String finishFilePath =
                pathBuilder.substring(0, pathBuilder.lastIndexOf(".ing")).toString();
        File finishFile = new File(finishFilePath);
        // one state finish.
        this.finishFile(finish, ingFile, finishFile);

        // stop
        if (recordBean.isNeedStop()) {
            LOGGER.warn("[WorkRecorder] operate failure , then program will shutdown");
            System.exit(-1);
        }

    }

    private void updateRecordLog(RecordLogBean recordLogBean) {
        LOGGER.debug("[WorkRecorder] do record log");

        if (recordLogBean == null) {
            LOGGER.warn("record log fail cause recordLogBean is null");
            return;
        }

        //transfer msg
        String line = recordLogBean.toString();

        //prepare path
        String bizDate = recordLogBean.getBusinessDate();
        String taskId = recordLogBean.getTaskId();
        StringBuilder pathBuilder = new StringBuilder(recordLogDir);
        pathBuilder.append(bizDate).append("/java/").append(taskId);
        String dir = pathBuilder.toString();
        File dirFile = new File(dir);
        dirFile.mkdirs();

        pathBuilder.append("task_exe_record_")
                .append(TimeUtils.getSimpleDate2(System.currentTimeMillis()))
                .append(".ing");

        File ingFile = new File(pathBuilder.toString());
        //write ing
        boolean finish = writeLine(ingFile, line);

        //rename ing to complete file
        String finishFilePath =
                pathBuilder.substring(0, pathBuilder.lastIndexOf(".ing")).toString();
        File finishFile = new File(finishFilePath);
        this.finishFile(finish, ingFile, finishFile);

    }

    private boolean writeLine(File dest, String line) {
        boolean finish = false;

        try (FileWriter fw = new FileWriter(dest)) {
            LOGGER.debug("write line ： \r\n " + line);
            fw.write(line);
            finish = true;
        } catch (IOException e) {
            LOGGER.error("writing record log fail. \r\n " + e.getMessage());
        }
        return finish;
    }

    private void finishFile(boolean finish, File ingFile, File destFile) {
        if (finish && ingFile.exists() && ingFile.isFile()) {
            if (ingFile.renameTo(destFile)) {
                LOGGER.debug("finish record log");
            } else {
                LOGGER.warn("finish record log fail");
            }
        }
    }
}
