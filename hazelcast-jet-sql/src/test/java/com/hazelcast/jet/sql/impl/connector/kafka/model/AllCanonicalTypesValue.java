/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.kafka.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * A class that has a field of every supported canonical type in SQL.
 */
@SuppressWarnings("unused") // getters-setters are used through reflection
public final class AllCanonicalTypesValue implements Serializable {

    private String string;
    private boolean boolean0;
    private byte byte0;
    private short short0;
    private int int0;
    private long long0;
    private BigDecimal bigDecimal;
    private float float0;
    private double double0;
    private LocalTime localTime;
    private LocalDate localDate;
    private LocalDateTime localDateTime;
    private OffsetDateTime offsetDateTime;

    public AllCanonicalTypesValue() {
    }

    @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:ExecutableStatementCount"})
    public AllCanonicalTypesValue(String string, boolean boolean0, byte byte0, short short0, int int0, long long0,
                                  BigDecimal bigDecimal, float float0, double double0, LocalTime localTime,
                                  LocalDate localDate, LocalDateTime localDateTime, OffsetDateTime offsetDateTime
    ) {
        this.string = string;
        this.boolean0 = boolean0;
        this.byte0 = byte0;
        this.short0 = short0;
        this.int0 = int0;
        this.long0 = long0;
        this.bigDecimal = bigDecimal;
        this.float0 = float0;
        this.double0 = double0;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.offsetDateTime = offsetDateTime;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public boolean isBoolean0() {
        return boolean0;
    }

    public void setBoolean0(boolean boolean0) {
        this.boolean0 = boolean0;
    }

    public byte getByte0() {
        return byte0;
    }

    public void setByte0(byte byte0) {
        this.byte0 = byte0;
    }

    public short getShort0() {
        return short0;
    }

    public void setShort0(short short0) {
        this.short0 = short0;
    }

    public int getInt0() {
        return int0;
    }

    public void setInt0(int int0) {
        this.int0 = int0;
    }

    public long getLong0() {
        return long0;
    }

    public void setLong0(long long0) {
        this.long0 = long0;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public float getFloat0() {
        return float0;
    }

    public void setFloat0(float float0) {
        this.float0 = float0;
    }

    public double getDouble0() {
        return double0;
    }

    public void setDouble0(double double0) {
        this.double0 = double0;
    }

    public LocalTime getLocalTime() {
        return localTime;
    }

    public void setLocalTime(LocalTime localTime) {
        this.localTime = localTime;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }

    public void setLocalDate(LocalDate localDate) {
        this.localDate = localDate;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public void setLocalDateTime(LocalDateTime localDateTime) {
        this.localDateTime = localDateTime;
    }

    public OffsetDateTime getOffsetDateTime() {
        return offsetDateTime;
    }

    public void setOffsetDateTime(OffsetDateTime offsetDateTime) {
        this.offsetDateTime = offsetDateTime;
    }

    @Override
    public String toString() {
        return "AllTypesValue{" +
                "string='" + string + '\'' +
                ", boolean0=" + boolean0 +
                ", byte0=" + byte0 +
                ", short0=" + short0 +
                ", int0=" + int0 +
                ", long0=" + long0 +
                ", bigDecimal=" + bigDecimal +
                ", float0=" + float0 +
                ", double0=" + double0 +
                ", localTime=" + localTime +
                ", localDate=" + localDate +
                ", localDateTime=" + localDateTime +
                ", offsetDateTime=" + offsetDateTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllCanonicalTypesValue that = (AllCanonicalTypesValue) o;
        return boolean0 == that.boolean0 &&
                byte0 == that.byte0 &&
                short0 == that.short0 &&
                int0 == that.int0 &&
                long0 == that.long0 &&
                Float.compare(that.float0, float0) == 0 &&
                Double.compare(that.double0, double0) == 0 &&
                Objects.equals(string, that.string) &&
                Objects.equals(bigDecimal, that.bigDecimal) &&
                Objects.equals(localTime, that.localTime) &&
                Objects.equals(localDate, that.localDate) &&
                Objects.equals(localDateTime, that.localDateTime) &&
                Objects.equals(offsetDateTime, that.offsetDateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string,
                boolean0,
                byte0,
                short0,
                int0,
                long0,
                bigDecimal,
                float0,
                double0,
                localTime,
                localDate,
                localDateTime,
                offsetDateTime
        );
    }
}
