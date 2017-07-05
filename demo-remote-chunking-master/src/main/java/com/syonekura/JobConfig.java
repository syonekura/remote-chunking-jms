package com.syonekura;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.batch.item.jms.JmsItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.jms.ChannelPublishingJmsMessageListener;
import org.springframework.integration.jms.DynamicJmsTemplate;
import org.springframework.integration.jms.JmsMessageDrivenEndpoint;
import org.springframework.integration.jms.JmsSendingMessageHandler;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;

import javax.jms.ConnectionFactory;

/**
 * Master se encarga de enviar requests a slaves y leer sus señales de ACK y respuestas simultaneamente:
 *
 * Job -> Flow -> ItemReader -> ItemWriter -> LocalRequestsChannel -> RemoteRequestsChannel
 *         |                     ^                                           |
 *         |               RepliesChannel <-    RemoteRepliesChannel     <--
 *         |
 *          -> RemoteChunkingResultsChannel -> JmsItemReader -> FlatFileItemWriter
 *
 * Created by syonekura on 19-04-17.
 */
@EnableBatchProcessing
@EnableIntegration
@Configuration
public class JobConfig {
    @StepScope
    @Bean
    ItemReader<String> itemReader(){
        return new FakeReader(100000, 20);
    }

    /**
     * Entrega ItemWriter encargado de enviar requests y recibir ACK's indicando que fueron recibidos exitosamente
     * @return ItemWriter
     */
    @StepScope
    @Bean
    ChunkMessageChannelItemWriter<String> itemWriter(){
        ChunkMessageChannelItemWriter<String> writer = new ChunkMessageChannelItemWriter<>();
        writer.setMessagingOperations(messagingTemplate());
        writer.setReplyChannel(repliesChannel());
        writer.setThrottleLimit(30);
        writer.setMaxWaitTimeouts(3000);
        return writer;
    }

    /**
     * Template de mensajes que hacen de wrapper para los requests
     * @return Template
     */
    @Bean
    MessagingTemplate messagingTemplate(){
        MessagingTemplate messagingTemplate = new MessagingTemplate();
        messagingTemplate.setDefaultChannel(requestsChannel());
        messagingTemplate.setReceiveTimeout(2000);
        return messagingTemplate;
    }

    /**
     * Canal que recibe directamente los requests del master
     * @return Canal subscribible
     */
    @Bean
    SubscribableChannel requestsChannel(){
        return new DirectChannel();
    }

    /**
     * Canal de respuesta (ACK's) de los slaves a master
     * @return queue
     */
    @Bean
    PollableChannel repliesChannel(){
        return new QueueChannel();
    }

    /**
     * Convierte los mensajes de master a mensajes en formato JMS
     * @return Handler de mensajes
     */
    @Bean
    @ServiceActivator(inputChannel = "requestsChannel")
    MessageHandler messageHandler(ConnectionFactory connectionFactory){
        JmsTemplate jmsTemplate = new DynamicJmsTemplate();
        jmsTemplate.setConnectionFactory(connectionFactory);
        JmsSendingMessageHandler jmsSendingMessageHandler = new JmsSendingMessageHandler(jmsTemplate);
        jmsSendingMessageHandler.setDestinationName("remoteChunkingRequestsChannel");
        return jmsSendingMessageHandler;
    }

    /**
     * Enpoint para procesar ACK's remotos
     * @return
     */
    @Bean
    JmsMessageDrivenEndpoint messageDrivenEndpoint() {
        return new JmsMessageDrivenEndpoint(listenerContainer(null), messageListener());
    }

    /**
     * Los ACK's remotos se guardan en la cola local
     * @return
     */
    @Bean
    ChannelPublishingJmsMessageListener messageListener() {
        ChannelPublishingJmsMessageListener listener = new ChannelPublishingJmsMessageListener();
        listener.setExpectReply(false);
        listener.setRequestChannel(repliesChannel());
        return listener;
    }

    /**
     * Contenedor de Listener que implementa el Endpoint de ACK's remotos
     * @param connectionFactory
     * @return
     */
    @Bean
    AbstractMessageListenerContainer listenerContainer(ConnectionFactory connectionFactory) {
        DefaultMessageListenerContainer listenerContainer = new DefaultMessageListenerContainer();
        listenerContainer.setConnectionFactory(connectionFactory);
        listenerContainer.setDestinationName("remoteChunkingRepliesChannel");
        listenerContainer.setConcurrentConsumers(1);
        listenerContainer.setMaxConcurrentConsumers(4);
        listenerContainer.setReceiveTimeout(5000);
        listenerContainer.setIdleTaskExecutionLimit(2);
        listenerContainer.setIdleConsumerLimit(1);
        listenerContainer.setMessageListener(messageListener());
        return listenerContainer;
    }

    @Bean
    Job remoteJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory){
        return jobBuilderFactory.get("Simple Remote Job")
                .start(flow(stepBuilderFactory)).end().build();
    }

    @Bean
    Flow flow(StepBuilderFactory stepBuilderFactory){
        Flow subFlow1 = new FlowBuilder<Flow>("RemoteSend")
                .from(remoteStep(stepBuilderFactory))
                .end();
        Flow subFlow2 = new FlowBuilder<Flow>("RemoteReceive")
                .from(writeFile(stepBuilderFactory))
                .end();
        return new FlowBuilder<Flow>("SplitFlow")
                .start(subFlow1).split(new SimpleAsyncTaskExecutor()).add(subFlow2).end();
    }

    @Bean
    Step remoteStep(StepBuilderFactory stepBuilderFactory){
        return stepBuilderFactory.get("Remote Step")
                .<String, String>chunk(100)
                .reader(itemReader())
                .writer(itemWriter())
                .build();
    }

    @StepScope
    @Bean
    JmsItemReader<String> jmsItemReader(ConnectionFactory connectionFactory){
        JmsTemplate template = new DynamicJmsTemplate();
        template.setConnectionFactory(connectionFactory);
        template.setDefaultDestinationName("remoteChunkingResultsChannel");
        template.setDeliveryPersistent(false);
        template.setExplicitQosEnabled(true);
        template.setReceiveTimeout(300);
        JmsItemReader<String> jmsItemReader = new JmsItemReader<>();
        jmsItemReader.setJmsTemplate(template);
        return jmsItemReader;
    }

    @StepScope
    @Bean
    FlatFileItemWriter<String> flatFileItemWriter(){
        FlatFileItemWriter<String> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setLineAggregator(new PassThroughLineAggregator<>());
        flatFileItemWriter.setResource(new FileSystemResource("/home/syonekura/output.csv"));
        return flatFileItemWriter;
    }

    @Bean
    Step writeFile(StepBuilderFactory stepBuilderFactory){
        return stepBuilderFactory.get("WriteOutputFile")
                .<String, String>chunk(100)
                .reader(jmsItemReader(null))
                .writer(flatFileItemWriter())
                .build();
    }
}
