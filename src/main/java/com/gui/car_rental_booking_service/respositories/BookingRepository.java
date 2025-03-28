package com.gui.car_rental_booking_service.respositories;

import com.gui.car_rental_booking_service.entities.Booking;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface BookingRepository extends JpaRepository<Booking, UUID> {



}
