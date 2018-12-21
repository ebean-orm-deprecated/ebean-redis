package domain;


import io.ebean.annotation.Cache;

import javax.persistence.Entity;
import java.time.LocalDate;

@Cache(enableQueryCache = true, nearCache = true)
@Entity
public class EFoo extends EBase {

  public enum Status {
    NEW,
    ACTIVE,
    INACTIVE
  }

  LocalDate localDate;

  Status status;

  String name;

  String notes;

  public EFoo() {
  }

  public EFoo(String name) {
    this.name = name;
    this.status = Status.NEW;
  }

  public String toString() {
    return "[id:" + id + " name:" + name + "date:" + localDate + ']';
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }

  public LocalDate getLocalDate() {
    return localDate;
  }

  public void setLocalDate(LocalDate localDate) {
    this.localDate = localDate;
  }
}
