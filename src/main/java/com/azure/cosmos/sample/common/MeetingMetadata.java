package com.azure.cosmos.sample.common;




import java.util.Set;

// @Data
// @Builder
public class MeetingMetadata {
    String id; // document key, also used as partition key
    Set<String> attendees;
    String startTime;
    String endTime;

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof MeetingMetadata)) {
            return false;
        }
        MeetingMetadata other = (MeetingMetadata) o;

        return  this.id.equals(other.id) && startTime.equals(startTime)
                && this.endTime.equals(other.endTime) && this.attendees.equals(other.attendees);
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    public Set<String> getAttendees() {
        return attendees;
    }

    public void setAttendees(Set<String> attendees) {
        this.attendees = attendees;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public MeetingMetadata() {

    }


    public MeetingMetadata(String id, Set<String> attendees, String startTime, String endTime) {
        this.id = id;
        this.attendees = attendees;
        this.startTime = startTime;
        this.endTime = endTime;
    }

}
