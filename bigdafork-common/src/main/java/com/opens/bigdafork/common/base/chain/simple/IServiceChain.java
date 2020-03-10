package com.opens.bigdafork.common.base.chain.simple;

/**
 * IServiceChain.
 * V is the last result of running this service chain.
 */
public interface IServiceChain<T, R, I, V> {

    IServiceChain<T, R, I, V> addChainPart(IChainPart<T, R> part);

    V run();

    void init(I i);

}
