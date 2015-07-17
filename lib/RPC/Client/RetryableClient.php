<?php
namespace RPC\Client;

use RPC\Errors\Constant;
use RPC\Errors\ServiceException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

class RetryableClient {
  private $maxRetry_;
  private $client_;
  private $httpClient_;

  public function __construct($client, $httpClient, $maxRetry = 1) {
    $this->client_ = $client;
    $this->httpClient_ = $httpClient;
    $this->maxRetry_ = $maxRetry;
  }

  public function __call($name, $arguments) {
    $method = new \ReflectionMethod($this->client_, $name);
    $queryString = 'type=' . $name;
    $this->httpClient_->setQueryString($queryString);
    $retry = 0;
    while (true) {
      $ex = null;
      try {
        return $method->invokeArgs($this->client_, $arguments);
      } catch (ServiceException $se) {
        $sleepMs = $this->backoffTime($se->errorCode);
        if ($retry >= $this->maxRetry_ || $sleepMs < 0) {
          throw $se;
        }
        usleep(1000 * ($sleepMs << $retry));
        $retry++;
      }
    }
    return null;
  }

  private function backoffTime($errorCode) {
    $backoffConf = Constant::get('ERROR_BACKOFF');
    if (array_key_exists($errorCode, $backoffConf)) {
      return $backoffConf[$errorCode];
    } else {
      return -1;
    }
  }
}