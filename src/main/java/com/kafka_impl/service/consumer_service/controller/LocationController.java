package com.kafka_impl.service.consumer_service.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka_impl.service.consumer_service.model.Location;
import config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consumer")
public class LocationController {


    Logger logger = LoggerFactory.getLogger(LocationController.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private String lastLocation = "";


    @GetMapping("/getLatestLocation")
    @CrossOrigin(origins = "*")
    public ResponseEntity<Location> getLatestLocation() throws JsonProcessingException {

        Location location = new Location();
        try {
            if(lastLocation.equals("")){
                location.setMessage(Constants.RIDER_NOT_STARTED);
                return  new ResponseEntity<>(location, HttpStatus.OK);
            }

            location = objectMapper.readValue(lastLocation, Location.class);


            return  new ResponseEntity<>(location, HttpStatus.OK);
        }catch (Exception e){
            location.setMessage(e.getMessage());
            return  new ResponseEntity<>(location, HttpStatus.INTERNAL_SERVER_ERROR);
        }


    }

    @KafkaListener(topics= Constants.LOCATION_UPDATE_TOPIC,groupId = Constants.GROUP_ID)
    public void updatedLocation(String value){
        logger.info(value);
        lastLocation = value;
    }


}
