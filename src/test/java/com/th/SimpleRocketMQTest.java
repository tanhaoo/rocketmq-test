package com.th;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class SimpleRocketMQTest {

    @Test
    public void producer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("mq1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("topic_test");
            message.setTags("tag_test");
            message.setBody(("It's body " + i).getBytes());
            message.setWaitStoreMsgOK(true);

//            // sync
//            SendResult send01 = producer.send(message);
//            // async
//            producer.send(message, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    System.out.println(sendResult);
//                }
//
//                @Override
//                public void onException(Throwable throwable) {
//
//                }
//            });
//            // custom
//            producer.sendOneway(message);

            MessageQueue queue = new MessageQueue("topic_test", "broker-a", 0);
            SendResult send02 = producer.send(message, queue);
            System.out.println(send02);
        }
    }

    @Test
    public void consumerPull() throws Exception {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("consumer_02");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.start();
        // 看这个namesrv上有几个message queue
        System.out.println("queues: ");
        Collection<MessageQueue> mqs = consumer.fetchMessageQueues("topic_test");
        mqs.forEach(messageQueue -> System.out.println(messageQueue));
        System.out.println("polling...");
        consumer.assign(mqs);
        List<MessageExt> poll = consumer.poll();
        poll.forEach(messageExt -> {
            System.out.println(new String(messageExt.getBody()));
        });

        //定向消费
//        List<MessageQueue> queues = new ArrayList<>();
//        queues.add(new MessageQueue("topic_test", "broker-a", 1));
//        consumer.assign(queues);
        //细粒度 到哪一个queue 第几个偏移量
//        consumer.seek(queues.get(0),4);
//        List<MessageExt> poll = consumer.poll();
//        poll.forEach(messageExt -> System.out.println(new String(messageExt.getBody())));

    }

    @Test
    public void consumerPush() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_01");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_test", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            msg.forEach(messageExt -> {
                System.out.println(messageExt);
                byte[] body = messageExt.getBody();
                System.out.println(new String(body));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.in.read();
    }

    @Test
    public void admin() throws Exception {
        //console,dashboard 基于admin
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr("localhost:9876");
        admin.start();
        TopicList topicList = admin.fetchAllTopicList();
        Set<String> sets = topicList.getTopicList();
        sets.forEach(e -> System.out.println(e));

        System.out.println("topic route | info");
        TopicRouteData topic_test = admin.examineTopicRouteInfo("topic_test");
        System.out.println(topic_test);
    }
}
