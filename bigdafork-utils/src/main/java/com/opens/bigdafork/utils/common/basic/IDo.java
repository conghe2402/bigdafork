package com.opens.bigdafork.utils.common.basic;

/**
 * abstract the action of doing anything.
 * 
 * @// TODO: 2020/1/6  this can be extended to a workflow framework.
 * @param <T>
 * @param <R>
 */
public interface IDo<T, R> {
    String REQUIREMENT = "OPENS.STRATEGY";
    R iDo(T t);

    //R exceptDo(T t);
}
