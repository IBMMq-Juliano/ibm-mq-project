package com.exemplo.ibmmq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IbmMqService {

    @Value("${ibm.mq.queueManager}")
    private String queueManager;

    @Value("${ibm.mq.channel}")
    private String channel;

    @Value("${ibm.mq.connName}")
    private String connName;

    @Value("${ibm.mq.queueName}")
    private String queueName;

    public void sendMessageToQueue(String messageText) throws MQException, IOException {
        MQQueueManager qMgr = null;
        MQQueue queue = null;

        try {
            com.ibm.mq.MQEnvironment.hostname = connName.split("\\(")[0];
            com.ibm.mq.MQEnvironment.port = Integer.parseInt(connName.split("\\(")[1].replace(")", ""));
            com.ibm.mq.MQEnvironment.channel = channel;

            qMgr = new MQQueueManager(queueManager);
            int openOptions = com.ibm.mq.MQC.MQOO_OUTPUT;
            queue = qMgr.accessQueue(queueName, openOptions);

            MQMessage message = new MQMessage();
            message.writeString(messageText);
            MQPutMessageOptions pmo = new MQPutMessageOptions();

            queue.put(message, pmo);
            System.out.println("Mensagem enviada para fila: " + queueName);
        } finally {
            if (queue != null) queue.close();
            if (qMgr != null) qMgr.disconnect();
        }
    }
}
