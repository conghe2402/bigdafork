package com.opens.bigdafork.utils.tools.shell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

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
        } finally {
            if (proc != null) {
                proc.destroy();
            }
        }
    }

    private ShellUtil() {}
}
