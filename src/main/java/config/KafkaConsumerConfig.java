package config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerConfig {

    private Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @KafkaListener(topics= Constants.LOCATION_UPDATE_TOPIC,groupId = Constants.GROUP_ID)
    public void updatedLocation(String value){
        logger.info(value);
    }

}
