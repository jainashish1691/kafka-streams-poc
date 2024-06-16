package com.examples.kafkastreams.dto;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Payment {

  private String fileId;
  private int id;
  private double amount;
  private String currency;
}
