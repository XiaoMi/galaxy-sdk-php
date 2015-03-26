<?php
/**
 * Created by IntelliJ IDEA.
 * User: lshangq
 * Date: 15-3-24
 * Time: 下午5:43
 */

namespace SDS\Client;


class ExactLimitScanner {
  public static function scan($tableClient, $scanRequest)
  {
    $baseWaitTime = 500 * 1000;
    $finished = false;
    $records = new \ArrayIterator();
    $retryTime = 0;
    $limit = $scanRequest->limit;
    while (!$finished) {
      if ($retryTime > 0) {
        usleep($baseWaitTime << ($retryTime - 1));
      }
      $result = $tableClient->scan($scanRequest);
      if (empty($result->nextStartKey)) {
        $finished = true;
      } else {
        $limit = $limit - count($result->records);
        if ($limit <= 0) {
          $finished = true;
        } else {
          $retryTime++;
          $scanRequest->limit = $limit;
          $scanRequest->startKey = $result->nextStartKey;
        }
      }
      foreach ($result->records as $record) {
        $records->append($record);
      }
    }
    return $records;
  }
} 