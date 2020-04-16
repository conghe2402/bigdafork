package com.opens.bigdafork.utils.tools.shell;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * shell utils.
 */
public final class ShellUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShellUtil.class);

    /**
     * exe shell cmd.
     * @param cmd
     */
    public static void execCommand(String cmd) throws Exception {
        LOGGER.debug("execute cmd : " + cmd);
        Process proc = null;
        //String[] cmds = {"source /opt/71hadoopclient/bigdata_env",
        //    "kinit -kt /opt/71hadoopclient/user.keytab ebda",
        //    cmd};
        try{
            proc = Runtime.getRuntime().exec(cmd);
            InputStream stderr =  proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stderr, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();
            while (line != null) {
                LOGGER.debug(line);
                line = br.readLine();
            }

            int ret = proc.waitFor();
            if (ret != 0) {
                LOGGER.error("err happened cause ret : " + ret);
            } else {
                LOGGER.info("cmd execute successfully.");
            }
        } finally {
            if (proc != null) {
                proc.destroy();
            }
        }
    }

    public static Map<String, CircularFifoBuffer> submitCmd(String cmd, String[] params) throws Exception {
        LOGGER.debug("execute cmd");
        Map<String, CircularFifoBuffer> result = new HashMap<>();

        Process proc = null;
        ProcessChannel normalReader = null;
        ProcessChannel errReader = null;
        try {
            if (params != null && params.length > 0) {
                proc = Runtime.getRuntime().exec(cmd,
                        params);
            } else {
                proc = Runtime.getRuntime().exec(cmd);
            }

            normalReader = new ProcessChannel("normalReader",
                    proc.getInputStream(),
                    true);

            errReader = new ProcessChannel("errReader",
                    proc.getErrorStream(),
                    false);

            normalReader.start();
            errReader.start();

            int runStatus = proc.waitFor();
            List<String> lastLine = normalReader.getLastLine();
            CircularFifoBuffer inputLines = normalReader.getInputLines();
            CircularFifoBuffer errLines = errReader.getInputLines();

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOGGER.error("weird sth");
            }

            LOGGER.debug("End up with : " + lastLine.get(0));
            result.put("0", inputLines);

            if (runStatus != 0) {
                result.put("1", errLines);
            }
            LOGGER.debug("runStatus : " + runStatus);
            return result;
        } finally {
            free(proc, normalReader, errReader);
        }
    }

    public static void free(Process proc,
                     ProcessChannel normalReader, ProcessChannel errReader) {
        if (proc != null) {
            proc.destroy();
            LOGGER.debug("proc destroyed");
        }

        if (normalReader != null && normalReader.isAlive()) {
            normalReader.interrupt();
            LOGGER.debug("normalReader interrupt");
        }

        if (errReader != null && errReader.isAlive()) {
            errReader.interrupt();
            LOGGER.debug("errReader interrupt");
        }
    }

    private ShellUtil() {

    }


    private static class ProcessChannel extends Thread {
        private String pcName;
        private boolean recordLastLine = false;
        private BufferedReader br;
        private InputStream inputStream;
        private CircularFifoBuffer inputLines = new CircularFifoBuffer(1000);
        private List<String> lastLine = new ArrayList<>(1);

        public ProcessChannel(String pcName, InputStream normalInputStream,
                              boolean recordLastLine) {
            this.inputStream = normalInputStream;
            this.pcName = pcName;
            this.recordLastLine = recordLastLine;
        }

        public CircularFifoBuffer getInputLines() {
            return inputLines;
        }

        public List<String> getLastLine() {
            return lastLine;
        }

        @Override
        public void run() {
            lastLine.add("run");
            try {
                br = new BufferedReader(
                        new InputStreamReader(inputStream, "UTF-8"));
                String line;
                while ((line = br.readLine()) != null) {
                    LOGGER.debug(pcName + " : " + line);
                    inputLines.add(line);
                    if (recordLastLine) {
                        lastLine.clear();
                        lastLine.add(line);
                    }
                }
            } catch (Exception e) {
                LOGGER.error(pcName + " exception : " + e.getMessage());
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
                    LOGGER.error(pcName + " close err : " + e.getMessage());
                }
            }
        }
    }
}
