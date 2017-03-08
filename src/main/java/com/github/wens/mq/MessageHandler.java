package com.github.wens.mq;

/**
 * Created by wens on 2017/3/7.
 */
public interface MessageHandler {

    void onMessage( byte[] data ) ;

}
