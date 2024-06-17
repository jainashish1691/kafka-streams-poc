package com.examples.kafkastreams.dto;

import java.io.Serializable;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class FileStatus implements Serializable {

  private int totalValidatedCount;
  private int invalidCount;
  private int validCount;
  private String fileId;


  public FileStatus() {
    this.totalValidatedCount = 0;
    this.invalidCount = 0;
    this.validCount = 0;
  }

  public FileStatus updateWith(PaymentValidationResult payment) {
    this.totalValidatedCount++;
    if (payment.isValid()) {
      this.validCount = validCount + 1;
    } else {
      this.invalidCount = invalidCount + 1;
    }
    return this;
  }

}
