package com.opens.bigdafork.utils.tools.yarn;

import com.opens.bigdafork.utils.common.util.DefaultConnect4Ready;
import com.opens.bigdafork.utils.tools.AbstractManageUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos;
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
        client.init(this.getConfiguration());
        client.start();
        LOGGER.info("connected to yarn : " + client);
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

    /**
     * Query max memory to be limited by max capacity of queue.
     * @param queueName
     * @return
     */
    public long queryMaxCapacityMemoryOfQueue(String queueName) {
        QueueInfo queueInfo = this.queryQueueInfo(queueName);
        long maxMemoryLimit = 0;
        if (queueInfo != null) {
            maxMemoryLimit = getLimitValue(queueInfo, "yarn.nodemanager.resource.memory-mb");
            LOGGER.info("maxMemoryLimit : " + maxMemoryLimit);
        } else {
            LOGGER.warn("get no queueInfo when queryMaxCapacityMemoryOfQueue[" + queueName + "]");
        }
        return maxMemoryLimit;
    }

    /**
     * Query max number of cores to be limited by max capacity of the specified queue.
     * @param queueName
     * @return
     */
    public long queryMaxCoresOfQueue(String queueName) {
        QueueInfo queueInfo = this.queryQueueInfo(queueName);
        long maxCoreLimit = 0;
        if (queueInfo != null) {
            maxCoreLimit = getLimitValue(queueInfo, "yarn.nodemanager.resource.cpu-vcores");
            LOGGER.info("maxCoreLimit : " + maxCoreLimit);
        } else {
            LOGGER.warn("get no queueInfo when queryMaxCapacityMemoryOfQueue[" + queueName + "]");
        }
        return maxCoreLimit;
    }

    public boolean checkoutQueueLackResource(String queueName) {
        QueueInfo queueInfo = this.queryQueueInfo(queueName);
        int nodeManagerNum = this.queryClusterMetrics().getNumNodeManagers();

        if (queueInfo == null) {
            LOGGER.warn("get no queueInfo when checkoutQueueLackResource [" + queueName + "]");
            return false;
        }

        int coresPerNode = Integer.parseInt(this.getConfiguration().get("yarn.nodemanager.resource.cpu-vcores"));
        long maxCoreLimit = (long)Math.ceil(queueInfo.getMaximumCapacity() * nodeManagerNum * coresPerNode);
        LOGGER.info("nodeManagerNum : " + nodeManagerNum + " ; coresPerNode : " + coresPerNode);
        LOGGER.info("maxCoreLimit : " + maxCoreLimit);

        //To be implemented
        //long allocatedCores = queueInfo.getQueueStatistics().getAllocatedVCores();
        long allocatedCores = 0;
        long limitCores = (long)Math.floor(maxCoreLimit * 0.96);
        if (limitCores <= allocatedCores) {
            LOGGER.warn("WARNING ! The number of cores will be not enough");
            return true;
        }

        int memPerNode = Integer.parseInt(this.getConfiguration().get("yarn.nodemanager.resource.memory-mb"));
        long maxMemLimit = (long)Math.ceil(queueInfo.getMaximumCapacity() * nodeManagerNum * memPerNode);
        LOGGER.info("nodeManagerNum : " + nodeManagerNum + " ; maxMemLimit : " + maxMemLimit);
        LOGGER.info("maxCoreLimit : " + maxCoreLimit);

        //To be implemented
        //long allocatedMem = queueInfo.getQueueStatistics().getAllocatedMemoryMB();
        long allocatedMem = 0;
        long limitMemory = (long)Math.floor(maxMemLimit * 0.98);
        if (limitMemory <= allocatedMem) {
            LOGGER.warn("WARNING ! The memory will be not enough");
            return true;
        }

        return false;
    }

    private long getLimitValue(QueueInfo queueInfo, String configItem) {
        //just number of all node managers, rather than number of active node manager.
        int nodeManagerNum = this.queryClusterMetrics().getNumNodeManagers();
        int valuePerNode = Integer.parseInt(this.getConfiguration().get(configItem));
        long limitValue = (long)Math.ceil(queueInfo.getMaximumCapacity() * nodeManagerNum * valuePerNode);
        LOGGER.info("nodeManager number : " + nodeManagerNum + " ; valuePerNode " + valuePerNode);
        return limitValue;

    }
}
