<?php

namespace SDS\Metrics;

use SDS\Admin\ClientMetrics;
use SDS\Admin\ClientMetricType;
use SDS\Admin\MetricData;

class RequestMetrics
{
  const EXECUTION_TIME = "ExecutionTime";

  private $queryString;
  private $metrics;

  public function __construct()
  {
    $this->metrics = array();
  }

  public function setQueryString($queryString)
  {
    $this->queryString = $queryString;
  }

  public function startEvent($metricName)
  {
    $timingInfo = new TimeInfo($this->millitime(), null);
    $this->metrics[$metricName] = $timingInfo;
  }

  public function endEvent($metricName)
  {
    $timingInfo = $this->metrics[$metricName];
    $timingInfo->setEndTimeMilli($this->millitime());
  }

  public function toClientMetrics()
  {
    $clientMetrics = new ClientMetrics();
    $clientMetrics->metricDataList = array();
    foreach ($this->metrics as $key => $value) {
      $metricData = new MetricData();
      if ($key == self::EXECUTION_TIME) {
        $metricData->metricName = $this->queryString . "." . $key;
        $metricData->clientMetricType = ClientMetricType::Letency;
        $metricData->value = $value->getEndTimeMilli() -
            $value->getStartTimeMilli();
        $metricData->timeStamp = round($value->getEndTimeMilli() / 1000);
      }
      $clientMetrics->metricDataList = $metricData;
    }

    return $clientMetrics;
  }

  private function millitime()
  {
    return round(microtime(true) * 1000);
  }
}

class TimeInfo
{
  private $startTimeMilli;
  private $endTimeMilli;

  public function __construct($startTimeMilli, $endTimeMilli)
  {
    $this->startTimeMilli = $startTimeMilli;
    $this->endTimeMilli = $endTimeMilli;
  }

  public function getStartTimeMilli()
  {
    return $this->startTimeMilli;
  }

  public function setStartTimeMilli($startTimeMilli)
  {
    $this->startTimeMilli = $startTimeMilli;
  }

  public function getEndTimeMilli()
  {
    return $this->endTimeMilli;
  }

  public function setEndTimeMilli($endTimeMilli)
  {
    $this->endTimeMilli = $endTimeMilli;
  }
}
