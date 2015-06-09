<?php

namespace SDS\Metrics;
require_once dirname(__DIR__) . "/Metrics/autoload.php";

use Exception;
use SDS\Admin\ClientMetrics;
use Thread;

class MetricsUploaderThread extends Thread
{
  const UPLOAD_INTERVAL = 60;

  public function __construct($adminServiceClient, $metricsQueue)
  {
    $this->adminServiceClient = $adminServiceClient;
    $this->metricsQueue = $metricsQueue;
  }

  public function run()
  {
    while (true) {
      try {
        $startTime = time();
        $clientMetrics = new ClientMetrics();
        $clientMetrics->metricDataList = array();
        $metrics = $this->metricsQueue->popAllMetrics();
        foreach ($metrics as $value) {
          $clientMetrics->metricDataList[] = $value;
        }
        $this->adminServiceClient->putClientMetrics($clientMetrics);
        $endTime = time();
        $usedTime = $endTime - $startTime;
        $leftTime = self::UPLOAD_INTERVAL - $usedTime;
        if ($leftTime > 0) {
          sleep($leftTime);
        }
      } catch (Exception $e) {
        // ignore
      }
    }
  }
}



