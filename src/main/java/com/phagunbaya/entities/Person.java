package com.phagunbaya.entities;

import java.io.Serializable;

/**
 * Created by phagunbaya on 12/04/16.
 */
public class Person implements Serializable {
  String id;
  String fname;
  String lname;
  Integer age;
  Long createTime;
  Long updateTime;
  String type;
  String random;

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFname() {
    return this.fname;
  }

  public void setFname(String fname) {
    this.fname = fname;
  }

  public String getLname() {
    return this.lname;
  }

  public void setLname(String lname) {
    this.lname = lname;
  }

  public Integer getAge() {
    return this.age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public Long getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getUpdateTime() {
    return this.updateTime;
  }

  public void setUpdateTime(Long updateTime) {
    this.updateTime = updateTime;
  }

  public String getType() {
    return this.type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getRandom() {
    return this.random;
  }

  public void setRandom(String random) {
    this.random = random;
  }
}
