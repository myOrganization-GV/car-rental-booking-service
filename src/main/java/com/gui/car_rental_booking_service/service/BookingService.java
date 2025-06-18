package com.gui.car_rental_booking_service.service;

import com.gui.car_rental_booking_service.entities.Booking;
import com.gui.car_rental_booking_service.enums.BookingStatus;
import com.gui.car_rental_booking_service.respositories.BookingRepository;
import com.gui.car_rental_common.commands.BookingCreationCommand;
import com.gui.car_rental_common.commands.CancelBookingCommand;
import com.gui.car_rental_common.dtos.BookingDto;
import com.gui.car_rental_common.events.booking.BookingCancellationFailedEvent;
import com.gui.car_rental_common.events.booking.BookingCancelledEvent;
import com.gui.car_rental_common.events.booking.BookingCreatedEvent;
import com.gui.car_rental_common.events.booking.BookingCreationFailedEvent;
import jakarta.persistence.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
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
        if (!bookingRepository.existsById(booking.getBookingId())) {
            throw new EntityNotFoundException("Booking not found with ID: " + booking.getBookingId());
        }
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

        logger.info("Received BookingCreationCommand");


        try {
            validateDate(command.getBookingDto());


            Booking booking = new Booking();
            Booking savedBooking = saveBookingFromCommand(command, booking);

            command.getBookingDto().setAmount(savedBooking.getTotalPrice());
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

    private Booking saveBookingFromCommand(BookingCreationCommand command, Booking booking) {
        booking.setCarId(command.getBookingDto().getCarId());
        booking.setRentalStartDate(command.getBookingDto().getRentalStartDate());
        booking.setRentalEndDate(command.getBookingDto().getRentalEndDate());
        booking.setBookingStatus(BookingStatus.PENDING);
        booking.setTotalPrice(command.getBookingDto().getPricePerDay());
        booking.setUserId(command.getBookingDto().getUserId());
        booking.setUserEmail(command.getBookingDto().getEmail());
        return bookingRepository.save(booking);
    }

    private static void validateDate(BookingDto bookingDto) {
        LocalDateTime now   = LocalDateTime.now();
        LocalDateTime start = bookingDto.getRentalStartDate();
        LocalDateTime end   = bookingDto.getRentalEndDate();

        if (start.isBefore(now.plusDays(1))) {
            throw new IllegalArgumentException(
                    String.format("Start date %s must be at least one day after now %s", start, now)
            );
        }
        if(end.isBefore(start.plusDays(1))){
            throw new IllegalArgumentException(
                    String.format("End date %s must be at least one day after start date %s", end, start)
            );
        }

        if (start.plusMonths(2).isBefore(end)) {
            throw new IllegalArgumentException(
                    String.format("Booking duration should not exceeds 2 months. Start date %s, End date %s", start, end)
            );
        }

    }


    @KafkaHandler
    public void consumeCancelBookingCommand(CancelBookingCommand command){
        logger.info("Received CancelBookingCommand from Saga Id: {}", command.getSagaTransactionId());
       try{
           Booking booking = bookingRepository.findById(command.getBookingDto().getBookingId()).orElseThrow();
           booking.setBookingStatus(BookingStatus.CANCELLED);
           Booking updatedBooking = bookingRepository.save(booking);
           BookingCancelledEvent bookingCancelledEvent = new BookingCancelledEvent(command.getSagaTransactionId(), command.getBookingDto());
           kafkaTemplate.send("booking-service-events", bookingCancelledEvent);
           logger.info("Booking cancelled with Booking Id: {} for Saga Id: {}",updatedBooking.getBookingId() , command.getSagaTransactionId());
       }catch(Exception e){
           logger.error("Error processing CancelBookingCommand: {}", e.getMessage(), e);

           BookingCancellationFailedEvent event = new BookingCancellationFailedEvent(
                   command.getSagaTransactionId(),
                   command.getBookingDto(),
                   e.getMessage()
           );
           kafkaTemplate.send("booking-service-events", event);

           logger.error("Cancel booking failed {}. Car still booked", e.getMessage());
       }

    }

}
