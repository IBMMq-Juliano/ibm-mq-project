package com.exemplo.ibmmq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class IBMMQExample {

    private static final String QMGR_SEFAZ = "QMSEFAZ";
    private static final String CHANNEL_SEFAZ = "ADMIN.CHL";
    private static final String CONN_NAME_SEFAZ = "localhost(1414)";
    private static final String QUEUE_NAME = "FILA1";

    private static final String QMGR_SERPRO = "QMSERPRO";
    private static final String CHANNEL_SERPRO = "ADMIN.CHL";
    private static final String CONN_NAME_SERPRO = "localhost(1515)";

    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                String timestamp = TIMESTAMP_FORMAT.format(new Date());
                String randomCode = generateRandomCode(10);
                String messageText = String.format("{\"timestamp\": \"%s\", \"message\": \"%s\"}", timestamp, randomCode);

                try {
                    // Enviar mensagem para o QMSEFAZ
                    putMessage(QMGR_SEFAZ, CHANNEL_SEFAZ, CONN_NAME_SEFAZ, messageText);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        // Agenda a execução da tarefa a cada 10 segundos
        timer.schedule(task, 0, 10000);
    }

    public static void putMessage(String qmgrName, String channelName, String connName, String messageText) {
        MQQueueManager qMgr = null;
        MQQueue queue = null;

        try {
            // Configuração do ambiente MQ
            com.ibm.mq.MQEnvironment.hostname = connName.split("\\(")[0];
            com.ibm.mq.MQEnvironment.port = Integer.parseInt(connName.split("\\(")[1].replace(")", ""));
            com.ibm.mq.MQEnvironment.channel = channelName;

            // Conectando ao Gerenciador de Filas
            qMgr = new MQQueueManager(qmgrName);
            System.out.println("Conectado ao Gerenciador de Filas: " + qmgrName);

            // Acessando a fila
            int openOptions = com.ibm.mq.MQC.MQOO_OUTPUT;
            queue = qMgr.accessQueue(QUEUE_NAME, openOptions);
            System.out.println("Fila aberta: " + QUEUE_NAME);

            // Criando a mensagem
            MQMessage message = new MQMessage();
            message.writeString(messageText);

            // Opções de envio
            MQPutMessageOptions pmo = new MQPutMessageOptions();

            // Enviando a mensagem
            queue.put(message, pmo);
            System.out.println("Mensagem enviada para fila " + QUEUE_NAME + ": " + messageText);

        } catch (MQException e) {
            System.err.println("Erro ao conectar ao MQ: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro de IO: " + e.getMessage());
        } finally {
            try {
                // Fechando a fila e desconectando
                if (queue != null) {
                    queue.close();
                }
                if (qMgr != null) {
                    qMgr.disconnect();
                }
                System.out.println("Desconectado do Gerenciador de Filas: " + qmgrName);
            } catch (MQException e) {
                System.err.println("Erro ao desconectar: " + e.getMessage());
            }
        }
    }

    private static String generateRandomCode(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < length; i++) {
            code.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return code.toString();
    }
}