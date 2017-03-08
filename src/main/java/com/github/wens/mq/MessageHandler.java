package com.github.wens.mq;

/**
 * Created by wens on 2017/3/7.
 */
public interface MessageHandler<T> {

    void onMessage( T message) ;
}
