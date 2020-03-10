package com.opens.bigdafork.datatask.execute;

import com.opens.bigdafork.common.base.observe.AbstractBasicObservable;
import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.works.WorkService;

/**
 * JobService.
 */
public final class JobService extends AbstractBasicObservable {

    /**
     * To reflect work instance and invoke common method.
     * To run task in tolerance mode,
     * and record result every time when we try to run task using any engine.
     * @param jobBean
     */
    public static void execute(JobBean jobBean) {
        //run task
        String clazz = jobBean.getClassName();

        try {
            WorkService ws = (WorkService)Class.forName(clazz).newInstance();
            ws.execute(jobBean);
        } catch (ClassNotFoundException
                | IllegalAccessException
                | InstantiationException e) {
            e.printStackTrace();
        }
    }

    private JobService() {}
}
