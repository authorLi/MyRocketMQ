package com.example.demo.client;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;

@Component
public class RocketMQClient {

    @Value("${apache.rocketmq.producer.ProducerGroup}")
    private String producerGroup;

    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    @PostConstruct
    public void RocketMQClient() {
        //生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        //指定NameServer地址，多个可以以分号(;)分隔开
        producer.setNamesrvAddr(namesrvAddr);
        try{
            /**
             * Producer 对象在使用之前一定要初始化，即调用start()方法，调用一次即可
             * 注意：不要再每次发送消息时都调用start()方法
             */
            producer.start();
            //构造消息对象，包含topic，tag和消息体
            Message message = new Message("TopicTest","Push","发送消息-----电波发送中。。。".getBytes(RemotingHelper.DEFAULT_CHARSET));
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            for (int i = 0;i < 10000;i++) {
                SendResult result = producer.send(message);
                System.out.println("发送响应MsgId:" + result.getMsgId() + ", 发送状态:" + result.getSendStatus());
            }
            stopWatch.stop();
            System.out.println("发送一万条需耗时:" + stopWatch.getTotalTimeMillis());
        } catch (Exception e) {
            e.getMessage();
        } finally {
            producer.shutdown();
        }

    }
}
