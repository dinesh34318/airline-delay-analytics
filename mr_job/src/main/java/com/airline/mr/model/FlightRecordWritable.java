package com.airline.mr.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stage 10: Reusable Writable for MapReduce Additive Flight Metrics
 *
 * Implements strict additive metrics:
 *  - flightCount: total flights
 *  - cancelledCount: flights cancelled (cancelled == 1)
 *  - validArrCount: flights with valid arrival delay
 *  - totalArrDelay: sum of arrival delays (minutes)
 *  - validDepCount: flights with valid departure delay
 *  - totalDepDelay: sum of departure delays (minutes)
 *  - delayed15Plus: count of flights delayed >= 15 min
 *  - totalDistance: sum of distances (miles)
 */
public class FlightRecordWritable implements Writable {

    private long   flightCount;
    private long   cancelledCount;
    private long   validArrCount;
    private double totalArrDelay;
    private long   validDepCount;
    private double totalDepDelay;
    private long   delayed15Plus;
    private double totalDistance;

    public FlightRecordWritable() {
        clear();
    }

    public void clear() {
        this.flightCount    = 0L;
        this.cancelledCount = 0L;
        this.validArrCount  = 0L;
        this.totalArrDelay  = 0.0;
        this.validDepCount  = 0L;
        this.totalDepDelay  = 0.0;
        this.delayed15Plus  = 0L;
        this.totalDistance  = 0.0;
    }

    public void set(long flightCount, long cancelledCount,
                    long validArrCount, double totalArrDelay,
                    long validDepCount, double totalDepDelay,
                    long delayed15Plus, double totalDistance) {
        this.flightCount    = flightCount;
        this.cancelledCount = cancelledCount;
        this.validArrCount  = validArrCount;
        this.totalArrDelay  = totalArrDelay;
        this.validDepCount  = validDepCount;
        this.totalDepDelay  = totalDepDelay;
        this.delayed15Plus  = delayed15Plus;
        this.totalDistance  = totalDistance;
    }

    public void merge(FlightRecordWritable other) {
        this.flightCount    += other.flightCount;
        this.cancelledCount += other.cancelledCount;
        this.validArrCount  += other.validArrCount;
        this.totalArrDelay  += other.totalArrDelay;
        this.validDepCount  += other.validDepCount;
        this.totalDepDelay  += other.totalDepDelay;
        this.delayed15Plus  += other.delayed15Plus;
        this.totalDistance  += other.totalDistance;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(flightCount);
        out.writeLong(cancelledCount);
        out.writeLong(validArrCount);
        out.writeDouble(totalArrDelay);
        out.writeLong(validDepCount);
        out.writeDouble(totalDepDelay);
        out.writeLong(delayed15Plus);
        out.writeDouble(totalDistance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flightCount    = in.readLong();
        this.cancelledCount = in.readLong();
        this.validArrCount  = in.readLong();
        this.totalArrDelay  = in.readDouble();
        this.validDepCount  = in.readLong();
        this.totalDepDelay  = in.readDouble();
        this.delayed15Plus  = in.readLong();
        this.totalDistance  = in.readDouble();
    }

    // Getters
    public long getFlightCount()       { return flightCount; }
    public long getCancelledCount()    { return cancelledCount; }
    public long getValidArrCount()     { return validArrCount; }
    public double getTotalArrDelay()   { return totalArrDelay; }
    public long getValidDepCount()     { return validDepCount; }
    public double getTotalDepDelay()   { return totalDepDelay; }
    public long getDelayed15Plus()     { return delayed15Plus; }
    public double getTotalDistance()   { return totalDistance; }

    public double getMeanArrDelay() {
        return validArrCount > 0 ? (totalArrDelay / validArrCount) : 0.0;
    }

    public double getMeanDepDelay() {
        return validDepCount > 0 ? (totalDepDelay / validDepCount) : 0.0;
    }

    public double getCancelRatePct() {
        return flightCount > 0 ? (100.0 * cancelledCount / flightCount) : 0.0;
    }

    public double getDelayed15Pct() {
        return flightCount > 0 ? (100.0 * delayed15Plus / flightCount) : 0.0;
    }
}
