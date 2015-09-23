<?php
namespace EMQ\Client;

use EMQ\Common\Version;
use RPC\Client\RpcTHttpClient;
use RPC\Common\ThriftProtocol;
use Thrift\Protocol\TCompactProtocol;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */
class EMQClientFactory {
  private $credential_;
  private $version_;
  private $httpClient_;

  /**
   * client端请求超时时间（ms）
   */
  const DEFAULT_CLIENT_TIMEOUT = 60000;

  /**
   * client端连接超时时间（ms）
   */
  const DEFAULT_CLIENT_CONN_TIMEOUT = 30000;

  /**
   * HTTP RPC服务地址
   */
  const DEFAULT_SERVICE_ENDPOINT = "http://emq.api.xiaomi.com";

  /**
   * HTTPS RPC服务地址
   */
  const DEFAULT_SECURE_SERVICE_ENDPOINT = "https://emq.api.xiaomi.com";

  /**
   * Queue操作RPC路径
   */
  const QUEUE_SERVICE_PATH = "/v1/api/queue";

  /**
   * Message操作RPC路径
   */
  const MESSAGE_SERVICE_PATH = "/v1/api/message";

  /**
   * 未设置retry或者未设置retry的次数时, retry次数的默认值
   */
  const DEFAULT_MAX_RETRY_TIME = 3;

  /**
   * @param \RPC\Auth\Credential $credential
   * Do automatic retry when curl reports CURLE_OPERATION_TIMEOUTED error,
   * note that the request may has been sent to the server successfully.
   * Don't set this when the operation is not idempotent.
   */
  public function __construct($credential) {
    $this->credential_ = $credential;
    $this->version_ = new Version();
  }

  /**
   * @return \EMQ\Queue\QueueServiceClient
   */
  public function newDefaultQueueClient() {
    return $this->newQueueClient(self::DEFAULT_SECURE_SERVICE_ENDPOINT);
  }

  /**
   * @param string $endpoint
   * @param int $timeout
   * @param int $connTimeout
   * @param bool $isRetry
   * @param int $maxRetry
   * @return EMQClient
   */
  public function newQueueClient($endpoint,
      $timeout = self::DEFAULT_CLIENT_TIMEOUT,
      $connTimeout = self::DEFAULT_CLIENT_CONN_TIMEOUT,
      $isRetry = false,
      $maxRetry = self::DEFAULT_MAX_RETRY_TIME) {
    $client = $this->getClient('EMQ\Queue\QueueServiceClient',
        $endpoint . self::QUEUE_SERVICE_PATH, $timeout, $connTimeout);
    $retryClient = new RetryableClient($client, $this->httpClient_, $isRetry, $maxRetry);
    return new EMQClient($retryClient);
  }

  /**
   * @return \EMQ\Message\MessageServiceClient
   */
  public function newDefaultMessageClient() {
    return $this->newMessageClient(self::DEFAULT_SECURE_SERVICE_ENDPOINT);
  }


  /**
   * @param string $endpoint
   * @param int $timeout
   * @param int $connTimeout
   * @param bool $isRetry
   * @param int $maxRetry
   * @return EMQClient
   */
  public function newMessageClient($endpoint,
      $timeout = self::DEFAULT_CLIENT_TIMEOUT,
      $connTimeout = self::DEFAULT_CLIENT_CONN_TIMEOUT,
      $isRetry = false,
      $maxRetry = self::DEFAULT_MAX_RETRY_TIME) {
    $client = $this->getClient('EMQ\Message\MessageServiceClient',
        $endpoint . self::MESSAGE_SERVICE_PATH, $timeout, $connTimeout);
    $retryClient = new RetryableClient($client, $this->httpClient_, $isRetry, $maxRetry);
    return new EMQClient($retryClient);
  }

  /**
   * @param $clientClass
   * @param $url
   * @param $timeout
   * @param $connTimeout
   * @return \EMQ\Queue\QueueServiceClient
   */
  protected function getClient($clientClass, $url, $timeout, $connTimeout) {
    $parts = parse_url($url);
    if (!isset($parts['port'])) {
      if ($parts['scheme'] === 'https') {
        $parts['port'] = 443;
      } else {
        $parts['port'] = 80;
      }
    }

    $httpClient = new RpcTHttpClient($this->credential_, $url, $timeout, $connTimeout,
        ThriftProtocol::TCOMPACT);
    $this->httpClient_ = $httpClient;
    $httpClient->addHeaders(array('User-Agent' => $this->userAgent()));

    $thriftProtocol = new TCompactProtocol($httpClient);
    return new $clientClass($thriftProtocol, $thriftProtocol);
  }

  private function userAgent() {
    return "PHP-SDK/" . $this->version() . " PHP/" . phpversion();
  }

  private function version() {
    $v = $this->version_;
    return "{$v->major}.{$v->minor}.{$v->revision}";
  }
}
