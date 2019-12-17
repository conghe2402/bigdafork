package com.opens.common.basic;

/**
 * abstract the action of doing anything.
 * @param <T>
 * @param <R>
 */
public interface IDo<T, R> {
    String REQUIREMENT = "OPENS.STRATEGY";
    R iDo(T t);
}
