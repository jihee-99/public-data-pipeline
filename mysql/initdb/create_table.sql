CREATE TABLE bus_arrival (
  `bus_id` varchar(45) NOT NULL,
  `station_id` varchar(45) DEFAULT NULL,
  `station_name` varchar(45) DEFAULT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime DEFAULT NULL,
  `avg_arrival1` double DEFAULT NULL,
  `avg_arrival2` double DEFAULT NULL,
  `lastupdate` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`bus_id`,`start_time`)
);