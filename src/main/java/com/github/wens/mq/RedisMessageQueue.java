package com.github.wens.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wens on 2017/3/7.
 */
public class RedisMessageQueue implements Runnable {

    private static Logger log = LoggerFactory.getLogger(RedisMessageQueue.class) ;

    private static String TOPIC_PREFIX = "TOPIC_%s" ;
    private static String EMPTY = "" ;

    private volatile boolean isStart = false ;

    private JedisPool jedisPool ;

    private ConcurrentHashMap<String,PullMessageWorker> pullMessageWorkers = new ConcurrentHashMap<String,PullMessageWorker>() ;



    public RedisMessageQueue(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public <T> void publish(String topic , byte[] data){
        Jedis jedis = jedisPool.getResource();
        try{
            Pipeline pipelined = jedis.pipelined();
            pipelined.rpush(String.format(TOPIC_PREFIX, topic ).getBytes(), data ) ;
            pipelined.publish("_queue_",topic );
            pipelined.sync();
        }finally {
            if(jedis != null ){
                jedis.close();
            }
        }

    }

    public void consume( String topic , MessageHandler messageHandler ){
        PullMessageWorker pullMessageWorker  = pullMessageWorkers.get(topic);
        if(pullMessageWorker == null ){
            PullMessageWorker newPullMessageWorker = new PullMessageWorker(topic);
            PullMessageWorker old = pullMessageWorkers.putIfAbsent(topic, newPullMessageWorker );
            if(old != null ){
                pullMessageWorker = old ;
            }else{
                pullMessageWorker = newPullMessageWorker ;
                pullMessageWorker.start();
            }
        }
        pullMessageWorker.addHandler(messageHandler);
    }

    public void start(){
        if(!isStart){
            synchronized (this){
                if(!isStart){
                    isStart = true ;
                    new Thread(this,"redis-mq-sub-thread").start();
                }
            }
        }
    }

    public void close(){

        isStart = false ;
        for(PullMessageWorker pullMessageWorker :pullMessageWorkers.values() ){
            pullMessageWorker.stop();
        }
        pullMessageWorkers.clear();
        jedisPool.close();
    }


    public void run() {

        while (isStart){

            try{
                Jedis jedis = jedisPool.getResource();
                TopicListener topicListener = new TopicListener();
                try{
                    jedis.subscribe( topicListener  , "_queue_" );
                }finally {
                    if(jedis != null ){
                        jedis.close();
                    }
                }
            }catch (Exception e){
                log.error("Redis has some error : \n {}" , e );
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    class TopicListener extends JedisPubSub{


        @Override
        public void onMessage(String channel, String message) {
            PullMessageWorker pullMessageWorker = pullMessageWorkers.get(message);
            if(pullMessageWorker != null ){
                synchronized (pullMessageWorker){
                    pullMessageWorker.notify();
                }

            }
        }

    }

    class PullMessageWorker implements Runnable {

        private String topic ;

        private volatile boolean stopped = false ;

        private List<MessageHandler> handlers ;

        public PullMessageWorker(String topic) {
            this.topic = topic ;
            handlers = new CopyOnWriteArrayList<MessageHandler>();
        }

        public void addHandler(MessageHandler messageHandler ){
            handlers.add(messageHandler);
        }

        public void run() {

            while(!stopped){

                while(true){

                    Jedis jedis = jedisPool.getResource();

                    byte[] data = null ;
                    try{
                        data = jedis.lpop(String.format(TOPIC_PREFIX, topic ).getBytes());
                    }finally {
                        if(jedis != null ){
                            jedis.close();
                        }
                    }

                    if(data == null ){
                        break;
                    }else{
                        executeHandler(data);
                    }


                }

                synchronized (this){
                    try {
                        this.wait(20000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

            }

        }

        private void executeHandler(byte[] data ) {
            for(MessageHandler handler : handlers ){
                try{
                    handler.onMessage(data);
                }catch (Exception e){
                    log.error("Execute handler fail :\n {} " , e );
                }

            }
        }

        public void start(){
            new Thread(this,"redis-mq-pull-" + topic ).start();
        }

        public void stop(){
            stopped = true ;
        }
    }




}
