/**
 * Put your copyright and license info here.
 */
package com.example.dedup;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.dedup.TimeBasedDedupOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

@ApplicationAnnotation(name="DedupTestApp")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafka = dag.addOperator("Kafka", KafkaSinglePortInputOperator.class);
    CsvParser parser = dag.addOperator("Parser", CsvParser.class);
    TimeBasedDedupOperator deduper = dag.addOperator("Deduper", TimeBasedDedupOperator.class);
    
    ConsoleOutputOperator unique = dag.addOperator("Unique", ConsoleOutputOperator.class);
    ConsoleOutputOperator duplicate = dag.addOperator("Duplicate", ConsoleOutputOperator.class);
    ConsoleOutputOperator expired = dag.addOperator("Expired", ConsoleOutputOperator.class);

    dag.addStream("Kafka-Parser", kafka.outputPort, parser.in);
    dag.addStream("Parser-Dedup", parser.out, deduper.input);
    dag.addStream("Dedup-Unique", deduper.unique, unique.input);
    dag.addStream("Dedup-Duplicate", deduper.duplicate, duplicate.input);
    dag.addStream("Dedup-Expired", deduper.expired, expired.input);
  }

}
