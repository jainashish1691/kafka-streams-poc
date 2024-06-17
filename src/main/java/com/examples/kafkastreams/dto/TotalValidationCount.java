package com.examples.kafkastreams.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
@NoArgsConstructor
public class TotalValidationCount {

  private String fileId;
  private long totalValid;
  private long totalInvalid;
}
