package com.opens.bigdafork.utils.tools.yarn;

import com.opens.bigdafork.utils.common.util.DefaultConnect4Ready;
import com.opens.bigdafork.utils.tools.AbstractManageUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * YarnManagerUtils.
 * 1.query cluster metrics.
 */
public class YarnManagerUtils extends AbstractManageUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(YarnManagerUtils.class);

    private YarnClient client;
    public YarnManagerUtils() {
        super(null);
    }

    public YarnManagerUtils(Properties properties) {
        super(properties);
    }

    @Override
    protected void initialize() {
        this.setConfiguration(new YarnConfiguration());
        DefaultConnect4Ready.ready4Yarn(this.getConfiguration());
        client = YarnClient.createYarnClient();
    }

    @Override
    public Configuration getEnvConfiguration() {
        return new YarnConfiguration(this.getConfiguration());
    }

    /**
     * Just query Number of NodeManager on the cluster.
     * @return
     */
    public YarnClusterMetrics queryClusterMetrics() {
        YarnClusterMetrics metrics = null;
        try {
            metrics = client.getYarnClusterMetrics();
        } catch (YarnException | IOException e) {
            LOGGER.error("Exception happened : " + e.getMessage());
        }
        return metrics;
    }

    /**
     * Query details of the specified queue by queue name.
     * @return
     */
    public QueueInfo queryQueueInfo(String queueName) {
        QueueInfo queueInfo = null;
        try {
            queueInfo = client.getQueueInfo(queueName);;
        } catch (YarnException | IOException e) {
            LOGGER.error("Exception happened : " + e.getMessage());
        }
        return queueInfo;
    }

}
