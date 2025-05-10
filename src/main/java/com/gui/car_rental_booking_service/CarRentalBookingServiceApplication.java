package com.gui.car_rental_booking_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class CarRentalBookingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CarRentalBookingServiceApplication.class, args);
	}

}
