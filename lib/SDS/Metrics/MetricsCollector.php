<?php

namespace SDS\Metrics;

use Thread;

class MetricsCollector {
  private $metricsQueue;
  private $metricsUploaderThread;

  public function __construct($adminServiceClient) {
    $this->metricsQueue = new MetricsQueue();
    $this->metricsUploaderThread = new MetricsUploaderThread($adminServiceClient,
        $this->metricsQueue);
  }

  public function __destruct() {
    $this->metricsUploaderThread->kill();
  }

  public function start() {
    $this->metricsUploaderThread->start();
  }

  public function collect($requestMetrics) {
    $this->metricsQueue->addAll($requestMetrics->toClientMetrics()
        ->metricDataList);
  }
}

