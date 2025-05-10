package com.gui.car_rental_booking_service.service;

import com.gui.car_rental_booking_service.entities.Booking;
import com.gui.car_rental_booking_service.enums.BookingStatus;
import com.gui.car_rental_booking_service.respositories.BookingRepository;
import com.gui.car_rental_common.commands.BookingCreationCommand;
import com.gui.car_rental_common.events.booking.BookingCreatedEvent;
import com.gui.car_rental_common.events.booking.BookingCreationFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@KafkaListener(topics = "rental-saga-booking-commands", groupId = "booking-service-group")
public class BookingService {
    private final BookingRepository bookingRepository;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final Logger logger = LoggerFactory.getLogger(BookingService.class);
    public BookingService(BookingRepository bookingRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.bookingRepository = bookingRepository;
        this.kafkaTemplate = kafkaTemplate;
    }
    public Optional<Booking> getBookingById(UUID bookingId) {
        return bookingRepository.findById(bookingId);
    }

    public List<Booking> getAllBookings() {
        return bookingRepository.findAll();
    }

    public Booking updateBooking(Booking booking) {
        return bookingRepository.save(booking);
    }

    public void deleteBooking(UUID bookingId) {
        bookingRepository.deleteById(bookingId);
    }

    public Booking confirmBooking(UUID bookingId) {
        Optional<Booking> bookingOptional = bookingRepository.findById(bookingId);
        if (bookingOptional.isPresent()) {
            Booking booking = bookingOptional.get();
            booking.setBookingStatus(BookingStatus.CONFIRMED);
            return bookingRepository.save(booking);
        }
        return null;
    }
    @KafkaHandler
    public Booking consumeBookingCreationCommand(BookingCreationCommand command){

        logger.info("Received BookingCreationCommand: {}", command);
        try {
            Booking booking = new Booking();
            booking.setCarId(command.getBookingDto().getCarId());
            booking.setRentalStartDate(command.getBookingDto().getRentalStartDate());
            booking.setRentalEndDate(command.getBookingDto().getRentalEndDate());
            booking.setBookingStatus(BookingStatus.PENDING);
            booking.calculateTotalPrice(command.getBookingDto().getPricePerDay());
            booking.setUserId(command.getBookingDto().getUserId());
            //just for testing failure
            //booking.setCarId(null);
            Booking savedBooking = bookingRepository.save(booking);
            command.getBookingDto().setAmount(booking.getTotalPrice());
            command.getBookingDto().setBookingId(savedBooking.getBookingId());
            BookingCreatedEvent bookingCreatedEvent = new BookingCreatedEvent(
                    command.getSagaTransactionId(), command.getBookingDto());
            kafkaTemplate.send("booking-service-events", bookingCreatedEvent);
            logger.info("Published BookingCreatedEvent for Saga ID: {}", command.getSagaTransactionId());

            return savedBooking;
        } catch (Exception e) {
            logger.error("Error processing BookingCreationCommand for Saga ID {}: {}",
                    command.getSagaTransactionId(), e.getMessage());
            BookingCreationFailedEvent failedEvent = new BookingCreationFailedEvent(command.getSagaTransactionId(), command.getBookingDto());
            failedEvent.setMessage("Booking creation failed: " + e.getMessage());
            kafkaTemplate.send("booking-service-events", failedEvent);

            logger.info("Published BookingCreationFailedEvent for Saga ID: {}", command.getSagaTransactionId());
            return null;
        }

    }

}
