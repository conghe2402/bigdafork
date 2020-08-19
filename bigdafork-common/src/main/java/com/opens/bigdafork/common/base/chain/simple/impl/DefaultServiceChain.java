package com.opens.bigdafork.common.base.chain.simple.impl;

import com.opens.bigdafork.common.base.chain.simple.IChainPart;
import com.opens.bigdafork.common.base.chain.simple.IServiceChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

/**
 * DefaultServiceChain.
 * 1.support proxy logic of every part .
 * 2.support extensibility for chain part logic.
 * 3.support workflow control in case of error.
 */
public class DefaultServiceChain<C extends ChainContext>
        implements IServiceChain<C, C, C, Object> {

    private static final Logger CHAIN_LOGGER = LoggerFactory.getLogger(DefaultServiceChain.class);
    private List<IChainPart<C, C>> chainHub = new ArrayList<>();

    private ProxyFactory proxyFactory = new ProxyFactory();
    private IProxyAddWork<C> proxyAddWork;
    private List<IErrorResovler> errorRelsList;
    private C context;

    public DefaultServiceChain() {

    }

    public DefaultServiceChain(IProxyAddWork<C> proxyAddWork,
                               List<IErrorResovler> errorRelsList) {
        this.proxyAddWork = proxyAddWork;
        this.errorRelsList = errorRelsList;
    }

    @Override
    public void init(C chainContext) {
        this.context = chainContext;
    }

    @Override
    public IServiceChain<C, C, C, Object> addChainPart(
            IChainPart<C, C> part) {
        if (proxyAddWork == null) {
            chainHub.add(part);
        } else {
            chainHub.add(proxyFactory.makePoxPart(part, proxyAddWork, errorRelsList));
        }
        return this;
    }

    /**
     * Modify t in iDo for changing t params.
     * @return
     */
    @Override
    public Object run() {
        C rc = context;
        for (IChainPart<C, C> chainPart : chainHub) {
            rc = chainPart.iDo(rc);
            if (rc.isExitSys()) {
                CHAIN_LOGGER.warn("chain operate failure , then program will shutdown.");
                System.exit(-1);
            }
            if (rc.getStop()) {
                break;
            }
        }
        return rc.get(ChainContext.RESULT_VALUE);
    }

    /**
     * ProxyFactory.
     */
    private class ProxyFactory {
        @SuppressWarnings({"rawtypes", "unchecked"})
        public IChainPart<C, C> makePoxPart(IChainPart<C, C> chainPartParam,
                                          IProxyAddWork proxyAddWorkParam,
                                            List<IErrorResovler> errorReslsParam) {
            ProxyChainPart proxyChainPart = new ProxyChainPart(chainPartParam, proxyAddWorkParam, errorReslsParam);
            ClassLoader loader = chainPartParam.getClass().getClassLoader();
            Class[] interfaces = chainPartParam.getClass().getSuperclass().getInterfaces();
            return (IChainPart<C, C>)Proxy.newProxyInstance(loader, interfaces, proxyChainPart);
        }
    }

    /**
     * ProxyChainPart. It is a proxy of ChainPart.
     */
    private class ProxyChainPart implements InvocationHandler {
        private IChainPart<C, C> chainPart;

        private IProxyAddWork<C> proxyAddWork;

        private List<IErrorResovler> errorResls;

        public ProxyChainPart(IChainPart<C, C> chainPart,
                              IProxyAddWork<C> proxyAddWork,
                              List<IErrorResovler> errorResls) {
            this.chainPart = chainPart;
            this.proxyAddWork = proxyAddWork;
            this.errorResls = errorResls;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object returnValue;
            if (this.proxyAddWork == null) {
                returnValue = method.invoke(this.chainPart, args);
            } else {
                C cc = (C)args[0];
                try {
                    this.proxyAddWork.doBefore(cc);
                    returnValue = method.invoke(this.chainPart, args);
                    this.proxyAddWork.doAfter(cc);
                } catch (Exception e) {
                    this.proxyAddWork.doException(cc, e);
                    returnValue = args[0];

                    //By accident spark client will be lost in 5min in case of lack of resources in the cluster
                    if (errorResls != null && errorResls.size() > 0) {
                        for (IErrorResovler resovler : errorResls) {
                            if (resovler.meet(e) && resovler.getResovle() == 1) {
                                returnValue = this.invoke(proxy, method, args);
                            }
                        }
                    }
                }
            }

            return returnValue;
        }
    }

    /**
     * What should proxy do.
     */
    public interface IProxyAddWork<C> {
        void doBefore(C arg);

        void doAfter(C arg);

        void doException(C arg, Exception e);
    }

    /**
     * How to control the work flow when exception encounter.
     */
    public interface IErrorResovler {
        boolean meet(Exception e);

        void setResolve(int res);

        int getResovle();
    }
}
