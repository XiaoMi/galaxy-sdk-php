<?php
namespace EMQ\Client;

use EMQ\Common\Constant;
use EMQ\Common\GalaxyEmqServiceException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */
class RetryableClient {
  private $isRetry_;
  private $maxRetry_;
  private $client_;
  private $httpClient_;

  public function __construct($client, $httpClient, $isRetry, $maxRetry) {
    $this->client_ = $client;
    $this->httpClient_ = $httpClient;
    $this->isRetry_ = $isRetry;
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
      } catch (GalaxyEmqServiceException $se) {
        $retryType = $this->getRetryType($se->errorCode, $name);
        $sleepMs = $this->backoffTime($se->errorCode, $name);
        if ($retry >= $this->maxRetry_ || $sleepMs < 0 || $retryType == -1 ||
            (!$this->isRetry_ && $retryType == 1)
        ) {
          throw $se;
        }
        if ($this->isRetry_ && $retryType == 1 || $retryType == 0) {
          usleep(1000 * ($sleepMs << $retry));
          $retry++;
        }
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

  private function getRetryType($errorCode, $name) {
    $retryTypeMap = Constant::get('ERROR_RETRY_TYPE');
    if (array_key_exists($errorCode, $retryTypeMap)) {
      $getRetryType = $retryTypeMap[$errorCode];
      if ($getRetryType == 2) {
        if ($this->startsWith($name, "deleteMessage") ||
            $this->startsWith($name, "changeMessage")
        ) {
          return 0;
        } else {
          return 1;
        }
      }
      return $getRetryType;
    }
    return -1;
  }

  private function startsWith($haystack, $needle) {
    $length = strlen($needle);
    return (substr($haystack, 0, $length) === $needle);
  }

}