package com.example.demo.server;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class RocketMQServer {

    @Value("${apache.rocketmq.consumer.PushConsumer}")
    private String consumerGroup;

    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    @PostConstruct
    public void DeaultMQPushConsumer() {
        //消费者组的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        //设置NameServer地址，多个可以以分号(;)隔开
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            //订阅TopicTest下tag为Push的消息
            consumer.subscribe("TopicTest","Push");
            //设置消费者从队列头消费还是队列尾开始消费
            //如果非第一次启动，则从上一次消费的位置继续消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
                try {
                    for (MessageExt messageExt : list) {
                        System.out.println("输出消息内容MessageExt:" + messageExt);
                        String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("消费响应MsgId:" + messageExt.getMsgId() + ", MsgBody:" + messageBody);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //稍后重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                //消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
