package com.opens.bigdafork.common.base.chain.simple.impl;

import com.opens.bigdafork.common.base.chain.simple.IChainPart;
import com.opens.bigdafork.common.base.chain.simple.IServiceChain;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

/**
 * DefaultServiceChain.
 * 1.support proxy logic of every part .
 * 2.support extensibility for chain part logic.
 */
public class DefaultServiceChain<C extends ChainContext>
        implements IServiceChain<C, C, C, Object> {

    private List<IChainPart<C, C>> chainHub = new ArrayList<>();

    private ProxyFactory proxyFactory = new ProxyFactory();
    private IProxyAddWork proxyAddWork;
    private C context;

    public DefaultServiceChain() {

    }

    public DefaultServiceChain(IProxyAddWork proxyAddWork) {
        this.proxyAddWork = proxyAddWork;
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
            chainHub.add(proxyFactory.makePoxPart(part, proxyAddWork));
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
        public IChainPart<C, C> makePoxPart(Object chainPartParam,
                                          IProxyAddWork proxyAddWorkParam) {
            ProxyChainPart proxyChainPart = new ProxyChainPart(chainPartParam, proxyAddWorkParam);
            ClassLoader loader = chainPartParam.getClass().getClassLoader();
            Class[] interfaces = chainPartParam.getClass().getSuperclass().getInterfaces();
            return (IChainPart<C, C>)Proxy.newProxyInstance(loader, interfaces, proxyChainPart);
        }
    }

    /**
     * ProxyChainPart. It is a proxy of ChainPart.
     */
    public class ProxyChainPart implements InvocationHandler {
        private Object chainPart;

        private IProxyAddWork proxyAddWork;

        public ProxyChainPart(Object chainPart, IProxyAddWork proxyAddWork) {
            this.chainPart = chainPart;
            this.proxyAddWork = proxyAddWork;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object returnValue;
            if (this.proxyAddWork == null) {
                returnValue = method.invoke(this.chainPart, args);
            } else {
                ChainContext cc = (ChainContext)args[0];
                try {
                    this.proxyAddWork.doBefore(cc);
                    returnValue = method.invoke(this.chainPart, args);
                    this.proxyAddWork.doAfter(cc);
                } catch (Exception e) {
                    this.proxyAddWork.doException(cc, e);
                    returnValue = args[0];
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
}
