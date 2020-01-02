package com.opens.bigdafork.utils.tools;

import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import com.opens.bigdafork.utils.common.util.FastjsonUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * This is an abstract class of management utilities.
 */
public abstract class AbstractManageUtils {

    @Setter @Getter
    private Configuration configuration = null;

    public AbstractManageUtils() {
        this(null);
    }

    public AbstractManageUtils(Properties properties) {
        if (properties != null) {
            String propertiesJsonStr = FastjsonUtils.convertObjectToJSONWithNullValue(properties);
            System.setProperty(BigdataUtilsGlobalConstants.JVM_CONTEXT_PROPERTIES, propertiesJsonStr);
        }
        initialize();
    }

    protected abstract void initialize();

}
