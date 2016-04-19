package com.phagunbaya.entities;

import java.io.Serializable;

/**
 * Created by phagunbaya on 12/04/16.
 */
public class Episode implements Serializable {
  String id;
  String label;
  Long time;
  Long endTime;
  String thing;

  public Long getTime() {
    return this.time;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  public Long getEndTime() {
    return this.endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getLabel() {
    return this.label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getThing() {
    return this.thing;
  }

  public void setThing(String thing){
    this.thing = thing;
  }

  public String getId() {
    return this.id;
  }

  public void setId(String id){
    this.id = id;
  }
}
