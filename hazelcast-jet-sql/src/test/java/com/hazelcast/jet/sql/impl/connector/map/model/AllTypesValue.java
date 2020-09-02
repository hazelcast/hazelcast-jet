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

package com.hazelcast.jet.sql.impl.connector.map.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;

/**
 * A class that has a field of every supported type in SQL.
 */
@SuppressWarnings("unused") // getters-setters are used through reflection
public final class AllTypesValue implements Serializable {

    public String string;
    private char character0;
    private Character character1;
    private boolean boolean0;
    private Boolean boolean1;
    private byte byte0;
    private Byte byte1;
    private short short0;
    private Short short1;
    private int int0;
    private Integer int1;
    private long long0;
    private Long long1;
    private BigDecimal bigDecimal;
    private BigInteger bigInteger;
    private float float0;
    private Float float1;
    private double double0;
    private Double double1;
    private LocalTime localTime;
    private LocalDate localDate;
    private LocalDateTime localDateTime;
    private Date date;
    private GregorianCalendar calendar;
    private Instant instant;
    private ZonedDateTime zonedDateTime;
    private OffsetDateTime offsetDateTime;

    public AllTypesValue() {
    }

    @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:ExecutableStatementCount"})
    public AllTypesValue(String string, char character0, Character character1, boolean boolean0, Boolean boolean1,
                         byte byte0, Byte byte1, short short0, Short short1, int int0, Integer int1, long long0,
                         Long long1, BigDecimal bigDecimal, BigInteger bigInteger, float float0, Float float1,
                         double double0, Double double1, LocalTime localTime, LocalDate localDate,
                         LocalDateTime localDateTime, Date date, GregorianCalendar calendar, Instant instant,
                         ZonedDateTime zonedDateTime, OffsetDateTime offsetDateTime
    ) {
        this.string = string;
        this.character0 = character0;
        this.character1 = character1;
        this.boolean0 = boolean0;
        this.boolean1 = boolean1;
        this.byte0 = byte0;
        this.byte1 = byte1;
        this.short0 = short0;
        this.short1 = short1;
        this.int0 = int0;
        this.int1 = int1;
        this.long0 = long0;
        this.long1 = long1;
        this.bigDecimal = bigDecimal;
        this.bigInteger = bigInteger;
        this.float0 = float0;
        this.float1 = float1;
        this.double0 = double0;
        this.double1 = double1;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.date = date;
        this.calendar = calendar;
        this.instant = instant;
        this.zonedDateTime = zonedDateTime;
        this.offsetDateTime = offsetDateTime;
    }

    public char getCharacter0() {
        return character0;
    }

    public void setCharacter0(char character0) {
        this.character0 = character0;
    }

    public Character getCharacter1() {
        return character1;
    }

    public void setCharacter1(Character character1) {
        this.character1 = character1;
    }

    public boolean isBoolean0() {
        return boolean0;
    }

    public void setBoolean0(boolean boolean0) {
        this.boolean0 = boolean0;
    }

    public Boolean getBoolean1() {
        return boolean1;
    }

    public void setBoolean1(Boolean boolean1) {
        this.boolean1 = boolean1;
    }

    public byte getByte0() {
        return byte0;
    }

    public void setByte0(byte byte0) {
        this.byte0 = byte0;
    }

    public Byte getByte1() {
        return byte1;
    }

    public void setByte1(Byte byte1) {
        this.byte1 = byte1;
    }

    public short getShort0() {
        return short0;
    }

    public void setShort0(short short0) {
        this.short0 = short0;
    }

    public Short getShort1() {
        return short1;
    }

    public void setShort1(Short short1) {
        this.short1 = short1;
    }

    public int getInt0() {
        return int0;
    }

    public void setInt0(int int0) {
        this.int0 = int0;
    }

    public Integer getInt1() {
        return int1;
    }

    public void setInt1(Integer int1) {
        this.int1 = int1;
    }

    public long getLong0() {
        return long0;
    }

    public void setLong0(long long0) {
        this.long0 = long0;
    }

    public Long getLong1() {
        return long1;
    }

    public void setLong1(Long long1) {
        this.long1 = long1;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public BigInteger getBigInteger() {
        return bigInteger;
    }

    public void setBigInteger(BigInteger bigInteger) {
        this.bigInteger = bigInteger;
    }

    public float getFloat0() {
        return float0;
    }

    public void setFloat0(float float0) {
        this.float0 = float0;
    }

    public Float getFloat1() {
        return float1;
    }

    public void setFloat1(Float float1) {
        this.float1 = float1;
    }

    public double getDouble0() {
        return double0;
    }

    public void setDouble0(double double0) {
        this.double0 = double0;
    }

    public Double getDouble1() {
        return double1;
    }

    public void setDouble1(Double double1) {
        this.double1 = double1;
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

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public GregorianCalendar getCalendar() {
        return calendar;
    }

    public void setCalendar(GregorianCalendar calendar) {
        this.calendar = calendar;
    }

    public Instant getInstant() {
        return instant;
    }

    public void setInstant(Instant instant) {
        this.instant = instant;
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
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
                ", character0=" + character0 +
                ", character1=" + character1 +
                ", boolean0=" + boolean0 +
                ", boolean1=" + boolean1 +
                ", byte0=" + byte0 +
                ", byte1=" + byte1 +
                ", short0=" + short0 +
                ", short1=" + short1 +
                ", int0=" + int0 +
                ", int1=" + int1 +
                ", long0=" + long0 +
                ", long1=" + long1 +
                ", bigDecimal=" + bigDecimal +
                ", bigInteger=" + bigInteger +
                ", float0=" + float0 +
                ", float1=" + float1 +
                ", double0=" + double0 +
                ", double1=" + double1 +
                ", localTime=" + localTime +
                ", localDate=" + localDate +
                ", localDateTime=" + localDateTime +
                ", date=" + date +
                ", calendar=" + calendar +
                ", instant=" + instant +
                ", zonedDateTime=" + zonedDateTime +
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
        AllTypesValue that = (AllTypesValue) o;
        return character0 == that.character0 &&
                boolean0 == that.boolean0 &&
                byte0 == that.byte0 &&
                short0 == that.short0 &&
                int0 == that.int0 &&
                long0 == that.long0 &&
                Float.compare(that.float0, float0) == 0 &&
                Double.compare(that.double0, double0) == 0 &&
                Objects.equals(string, that.string) &&
                Objects.equals(character1, that.character1) &&
                Objects.equals(boolean1, that.boolean1) &&
                Objects.equals(byte1, that.byte1) &&
                Objects.equals(short1, that.short1) &&
                Objects.equals(int1, that.int1) &&
                Objects.equals(long1, that.long1) &&
                Objects.equals(bigDecimal, that.bigDecimal) &&
                Objects.equals(bigInteger, that.bigInteger) &&
                Objects.equals(float1, that.float1) &&
                Objects.equals(double1, that.double1) &&
                Objects.equals(localTime, that.localTime) &&
                Objects.equals(localDate, that.localDate) &&
                Objects.equals(localDateTime, that.localDateTime) &&
                Objects.equals(date, that.date) &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(instant, that.instant) &&
                Objects.equals(zonedDateTime, that.zonedDateTime) &&
                Objects.equals(offsetDateTime, that.offsetDateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string,
                character0,
                character1,
                boolean0,
                boolean1,
                byte0,
                byte1,
                short0,
                short1,
                int0,
                int1,
                long0,
                long1,
                bigDecimal,
                bigInteger,
                float0,
                float1,
                double0,
                double1,
                localTime,
                localDate,
                localDateTime,
                date,
                calendar,
                instant,
                zonedDateTime,
                offsetDateTime
        );
    }
}
