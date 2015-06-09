<?php

namespace SDS\Metrics;

use Mutex;
use Stackable;

class MetricsQueue extends Stackable {
  public function __construct() {
    $this->mutex = Mutex::create();
    $this->clear();
  }

  public function add($metricData) {
    Mutex::lock($this->mutex);
    $this[] = $metricData;
    Mutex::unlock($this->mutex);
  }

  public function addAll($metricDataList) {
    Mutex::lock($this->mutex);
    foreach($metricDataList as $value) {
      $this[] = $value;
    }
    Mutex::unlock($this->mutex);
  }

  public function popAllMetrics() {
    Mutex::lock($this->mutex);
    $metrics = [];
    foreach ($this as $value) {
      $metrics[] = $value;
    }
    $this->clear();
    Mutex::unlock($this->mutex);
    return $metrics;
  }

  protected function clear() {
    foreach ($this as $i => $value) {
      unset($this[$i]);
    }
  }

  public function run() {
  }
}
