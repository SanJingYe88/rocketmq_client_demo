package it.com.mq.producer;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Title: producer发送异步消息
 * @Description:
 * @date 2022/8/1711:34
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        // 创建DefaultMQProducer类并设定生产者名称
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("producer-group-async");
        // 设置NameServer地址，如果是集群的话，使用分号;分隔开
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        // 消息最大长度 默认4M
        defaultMQProducer.setMaxMessageSize(4096);
        // 发送消息超时时间，默认3000ms
        defaultMQProducer.setSendMsgTimeout(3000);
        // 发送消息失败重试次数，默认2
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(2);
        // 启动消息生产者
        defaultMQProducer.start();
        for (int i = 0; i < 5; i++) {
            String msg = "AsyncProducer发送异同步消息:" + i;
            String topicName = "SimpleTopic";
            String tagName = "async";
            byte[] msgBytes = msg.getBytes(RemotingHelper.DEFAULT_CHARSET);
            // 创建消息，并指定Topic(主题)，Tag(标签)和消息内容
            Message message = new Message(topicName, tagName, msgBytes);
            // 发送同步消息到一个Broker，可以通过sendResult返回消息是否成功送达
            System.out.println(msg + ",消息发送时间:" + new Date());
            defaultMQProducer.send(message, new SendCallback() {
                // 消息发送成功的回调,发送成功指消息成功发送到RocketMQ Broker中.
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(msg + ",消息发送结果：" + sendResult + ",消息结果时间：" + new Date());
                }

                // 发送失败的回调
                @Override
                public void onException(Throwable throwable) {
                    System.out.println(msg + ",消息发送失败了：" + throwable.getMessage() + ",消息结果时间：" + new Date());
                }
            });
        }

        // 如果不再发送消息，关闭Producer实例
        defaultMQProducer.shutdown();
    }
}
