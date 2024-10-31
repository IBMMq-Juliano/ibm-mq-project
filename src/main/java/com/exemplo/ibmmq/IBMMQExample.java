package com.exemplo.ibmmq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A classe IBMMQExample é responsável por enviar mensagens JSON a múltiplas filas no IBM MQ de forma periódica.
 * A cada 10 segundos, um código aleatório e um timestamp são gerados e enviados como mensagem JSON 
 * para as filas especificadas.
 *
 * Entradas:
 *  - Não requer entradas externas para sua execução.
 *
 * Saídas:
 *  - Envia mensagens JSON contendo timestamp e código aleatório para filas no IBM MQ.
 *
 * A classe utiliza bibliotecas do IBM MQ para estabelecer a conexão e enviar mensagens, 
 * e classes padrão Java para formatação de data, geração de códigos aleatórios e agendamento de tarefas.
 */
public class IBMMQExample {

    // Definições de conexão e filas para o Gerenciador de Filas QMSEFAZ
    private static final String QMGR_SEFAZ = "QMSEFAZ";
    private static final String CHANNEL_SEFAZ = "ADMIN.CHL";
    private static final String CONN_NAME_SEFAZ = "localhost(1414)";
    private static final String QUEUE_NAME = "FILA1";

    // Definições de conexão e filas para o Gerenciador de Filas QMSERPRO
    private static final String QMGR_SERPRO = "QMSERPRO";
    private static final String CHANNEL_SERPRO = "ADMIN.CHL";
    private static final String CONN_NAME_SERPRO = "localhost(1515)";

    // Formato de data para o timestamp nas mensagens
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Random RANDOM = new Random();

    /**
     * Método principal para execução do programa. Configura e inicia um agendador que 
     * executa a tarefa de envio de mensagens a cada 10 segundos.
     *
     * @param args Argumentos da linha de comando (não são utilizados)
     */
    public static void main(String[] args) {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                String timestamp = TIMESTAMP_FORMAT.format(new Date());
                String randomCode = generateRandomCode(10);
                String messageText = String.format("{\"timestamp\": \"%s\", \"message\": \"%s\"}", timestamp, randomCode);

                try {
                    // Envia mensagem para o Gerenciador de Filas QMSEFAZ
                    putMessage(QMGR_SEFAZ, CHANNEL_SEFAZ, CONN_NAME_SEFAZ, messageText);

                    // Envia mensagem para o Gerenciador de Filas QMSERPRO
                    putMessage(QMGR_SERPRO, CHANNEL_SERPRO, CONN_NAME_SERPRO, messageText);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        // Agenda a execução da tarefa de envio de mensagem a cada 10 segundos
        timer.schedule(task, 0, 10000);
    }

    /**
     * Envia uma mensagem para a fila especificada no IBM MQ.
     *
     * @param qmgrName   Nome do Gerenciador de Filas
     * @param channelName Nome do canal de comunicação
     * @param connName    Nome e porta da conexão no IBM MQ
     * @param messageText Texto da mensagem que será enviada
     */
    public static void putMessage(String qmgrName, String channelName, String connName, String messageText) {
        MQQueueManager qMgr = null;
        MQQueue queue = null;

        try {
            // Configuração do ambiente MQ
            com.ibm.mq.MQEnvironment.hostname = connName.split("\\(")[0];
            com.ibm.mq.MQEnvironment.port = Integer.parseInt(connName.split("\\(")[1].replace(")", ""));
            com.ibm.mq.MQEnvironment.channel = channelName;

            // Conecta ao Gerenciador de Filas
            qMgr = new MQQueueManager(qmgrName);
            System.out.println("Conectado ao Gerenciador de Filas: " + qmgrName);

            // Acessa a fila no modo de saída (output)
            int openOptions = com.ibm.mq.MQC.MQOO_OUTPUT;
            queue = qMgr.accessQueue(QUEUE_NAME, openOptions);
            System.out.println("Fila aberta: " + QUEUE_NAME);

            // Cria a mensagem que será enviada
            MQMessage message = new MQMessage();
            message.writeString(messageText);

            // Define as opções de envio para a mensagem
            MQPutMessageOptions pmo = new MQPutMessageOptions();

            // Envia a mensagem para a fila
            queue.put(message, pmo);
            System.out.println("Mensagem enviada para fila " + QUEUE_NAME + ": " + messageText);

        } catch (MQException e) {
            System.err.println("Erro ao conectar ao MQ: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro de IO: " + e.getMessage());
        } finally {
            try {
                // Fecha a fila e desconecta do Gerenciador de Filas
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

    /**
     * Gera um código aleatório de caracteres alfanuméricos.
     *
     * @param length Comprimento do código aleatório a ser gerado
     * @return Código aleatório como uma String
     */
    private static String generateRandomCode(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < length; i++) {
            code.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return code.toString();
    }
}
