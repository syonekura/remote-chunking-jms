package com.syonekura;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.item.jms.JmsItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.jms.ChannelPublishingJmsMessageListener;
import org.springframework.integration.jms.DynamicJmsTemplate;
import org.springframework.integration.jms.JmsMessageDrivenEndpoint;
import org.springframework.integration.jms.JmsSendingMessageHandler;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

import javax.jms.ConnectionFactory;

/**
 * Configura el flujo de trabajo de los esclavos, el flujo es el siguiente:
 *
 * RemoteChunkRequestChannel -> localChunkRequests -> ChunkHandler -> localChunkACKs -> remoteChunkACKs
 *                                                                \
 *                                                                 -> remoteChunkResponses
 *
 * Created by syonekura on 19-04-17.
 */
@Slf4j
@Configuration
@EnableBatchProcessing
@EnableIntegration
public class SlaveConfig {

    /**
     * Endpoint para recibir mensajes de RemoteMaster
     * @return Endpoint activado por mensajes
     */
    @Bean
    JmsMessageDrivenEndpoint messageDrivenEndpoint(){
        return new JmsMessageDrivenEndpoint(listenerContainer(null), messageListener());
    }

    /**
     * Listener que hace de nexo entre la cola remota y local de master
     * @param connectionFactory ConnectionFactory
     * @return contenedor de listeners
     */
    @Bean
    AbstractMessageListenerContainer listenerContainer(ConnectionFactory connectionFactory){
        DefaultMessageListenerContainer listenerContainer = new DefaultMessageListenerContainer();
        listenerContainer.setConnectionFactory(connectionFactory);
        listenerContainer.setDestinationName("remoteChunkingRequestsChannel");
        listenerContainer.setConcurrency("10-50");
        listenerContainer.setReceiveTimeout(5000);
        listenerContainer.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER);
        listenerContainer.setMessageListener(messageListener());
        return listenerContainer;
    }

    /**
     * Listener que se encarga de transformar los mensajes remotos en objetos de la cola local
     * @return Listener para agregar al listener container
     */
    @Bean
    ChannelPublishingJmsMessageListener messageListener() {
        ChannelPublishingJmsMessageListener listener = new ChannelPublishingJmsMessageListener();
        listener.setExpectReply(false);
        listener.setRequestChannel(localChunkRequests());
        return listener;
    }

    /**
     * Cola local de requests de master, ie implementa LocalMaster. Acá llegan los mensajes del messageListener
     * @return Canal subscribible
     */
    @Bean
    SubscribableChannel localChunkRequests(){
        return new DirectChannel();
    }

    /**
     * Bean encargado de realizar el procesamiento de los items en la cola de requests locales de master
     * @return instancia de ChunkProcessorChunkHandler
     */
    @Bean
    @ServiceActivator(inputChannel = "localChunkRequests", outputChannel = "localChunkACKs")
    ChunkProcessorChunkHandler<String> chunkHandler(){
        ChunkProcessorChunkHandler<String> handler = new ChunkProcessorChunkHandler<>();
        handler.setChunkProcessor(chunkProcessor(null));
        return handler;
    }

    /**
     * ChunkProcessor que escribe los resultados via JMS
     * @param processor Processador
     * @return ChunkProcessor
     */
    @Bean
    ChunkProcessor<String> chunkProcessor(SimpleStringProcessor processor){
        return new SimpleChunkProcessor<>(processor, jmsItemWriter());
    }

    /**
     * Canal de local de replies de slave (ACK)
     * @return Canal de respuesta de slave
     */
    @Bean
    SubscribableChannel localChunkACKs(){
        return new DirectChannel();
    }

    /**
     * ServiceActivator para transferir ACK's desde canal local a canal remoto
     * @param connectionFactory ConnectionFactory
     * @return ServiceActivator
     */
    @Bean
    @ServiceActivator(inputChannel = "localChunkACKs")
    MessageHandler messageHandler(ConnectionFactory connectionFactory){
        JmsTemplate jmsTemplate = new DynamicJmsTemplate();
        jmsTemplate.setConnectionFactory(connectionFactory);
        JmsSendingMessageHandler messageHandler = new JmsSendingMessageHandler(jmsTemplate);
        messageHandler.setDestinationName("remoteChunkingRepliesChannel");
        return messageHandler;
    }

    /**
     * ItemWriter del procesador de chunks
     * @return JmsItemWriter
     */
    @Bean
    JmsItemWriter<String> jmsItemWriter(){
        JmsItemWriter<String> itemWriter = new JmsItemWriter<>();
        itemWriter.setJmsTemplate(writerTemplate(null));
        return itemWriter;
    }

    /**
     * JmsTemplate escribe directo a la cola remota de respuestas de slave a master
     * @param connectionFactory ConnectionFactory
     * @return JmsTemplate configurado para escribir en la cola de respuestas
     */
    @Bean
    JmsTemplate writerTemplate(ConnectionFactory connectionFactory){
        JmsTemplate template = new DynamicJmsTemplate();
        template.setConnectionFactory(connectionFactory);
        template.setDefaultDestinationName("remoteChunkingResultsChannel");
        template.setDeliveryPersistent(false);
        template.setExplicitQosEnabled(true);
        return template;
    }
}
