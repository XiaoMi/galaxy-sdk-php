<?php
/**
 * User: heliangliang
 * Date: 5/13/14
 * Time: 9:00 PM
 */

namespace SDS\Client;

use SDS\Common\ThriftProtocol;
use SDS\Common\Version;
use SDS\Errors\Constant;
use SDS\Errors\ServiceException;
use SDS\Metrics\MetricsCollector;
use SDS\Metrics\RequestMetrics;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Protocol\TJSONProtocol;
use Thrift\Protocol\TCompactProtocol;

class ClientFactory
{
  private $credential_;
  private $version_;
  private $verbose_;
  private $httpClient_;
  private $protocol_;
  protected $retryIfOperationTimeout_;
  private $metricsCollector_;
  private $isMetricEnabled_;

  /**
   * @param \SDS\Auth\Credential $credential
   * @param bool $retryIfOperationTimeout
   * Do automatic retry when curl reports CURLE_OPERATION_TIMEOUTED error,
   * note that the request may has been sent to the server successfully.
   * Don't set this when the operation is not idempotent.
   * @param bool $verbose
   */
  public function __construct($credential, $retryIfOperationTimeout = false,
                              $verbose = false, $isMetricEnabled = false,
                              $protocol = ThriftProtocol::TBINARY
  )
  {
    $this->credential_ = $credential;
    $this->version_ = new Version();
    $this->verbose_ = $verbose;
    $this->protocol_ = $protocol;
    $this->retryIfOperationTimeout_ = $retryIfOperationTimeout;
    $this->isMetricEnabled_ = $isMetricEnabled;
    $this->metricsCollector_ = null;
  }

  /**
   * @return \SDS\Auth\AuthServiceClient OAuth token generation service client
   */
  public function newDefaultAuthClient($supportAccountKey = false)
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
        Constant::get('TABLE_AUTH_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newAuthClient($url, $timeout, $connTimeout, $supportAccountKey);
  }


  /**
   * @param $url string the authentication endpoint url
   * @return \SDS\Auth\AuthServiceClient OAuth token generation service client
   */
  public function newAuthClient($url, $timeout, $connTimeout, $supportAccountKey = false)
  {
    $client = $this->getClient('SDS\Auth\AuthServiceClient', $url, $timeout, $connTimeout,
        $supportAccountKey);
    return new RetryableClient($client, $this->httpClient_, $this->metricsCollector_);
  }

  /**
   * @return \SDS\Admin\AdminServiceClient
   */
  public function newDefaultAdminClient($supportAccountKey = false)
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
        Constant::get('ADMIN_SERVICE_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newAdminClient($url, $timeout, $connTimeout, $supportAccountKey);
  }

  /**
   * @param $url string the administration endpoint url
   * @return \SDS\Admin\AdminServiceClient
   */
  public function newAdminClient($url, $timeout, $connTimeout, $supportAccountKey = false)
  {
    $client = $this->getClient('SDS\Admin\AdminServiceClient', $url, $timeout, $connTimeout,
        $supportAccountKey);
    return new RetryableClient($client, $this->httpClient_, $this->metricsCollector_);
  }

  /**
   * @return \SDS\Table\TableServiceClient
   */
  public function newDefaultTableClient($supportAccountKey = false)
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
        Constant::get('TABLE_SERVICE_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newTableClient($url, $timeout, $connTimeout, $supportAccountKey);
  }

  /**
   * @param $url string the table access endpoint url
   * @return \SDS\Table\TableServiceClient
   */
  public function newTableClient($url, $timeout, $connTimeout, $supportAccountKey = false)
  {
    $client = $this->getClient('SDS\Table\TableServiceClient', $url, $timeout, $connTimeout,
        $supportAccountKey);
    return new RetryableClient($client, $this->httpClient_, $this->metricsCollector_);
  }

  protected function getClient($clientClass, $url, $timeout, $connTimeout, $supportAccountKey)
  {
    $parts = parse_url($url);
    if (!isset($parts['port'])) {
      if ($parts['scheme'] === 'https') {
        $parts['port'] = 443;
      } else {
        $parts['port'] = 80;
      }
    }
    $httpClient = new SdsTHttpClient($this->credential_, $url, $timeout, $connTimeout,
        $this->protocol_, $this->retryIfOperationTimeout_, $this->verbose_);
    $httpClient->setSupportAccountKey($supportAccountKey);
    $this->httpClient_ = $httpClient;
    $httpClient->addHeaders(array('User-Agent' => $this->userAgent()));

    $protocolMap = \SDS\Common\Constant::get('THRIFT_PROTOCOL_MAP');
    $protocolClass = new \ReflectionClass('Thrift\Protocol\\' . $protocolMap[$this->protocol_]);
    $thriftProtocol = $protocolClass->newInstanceArgs(array('trans' => $httpClient));
    if ($this->isMetricEnabled_ && $this->metricsCollector_ == null) {
      $adminClientClass = 'SDS\Admin\AdminServiceClient';
      $getAdminClient = new $adminClientClass($thriftProtocol, $thriftProtocol);
      $metricAdminClient = new RetryableClient($getAdminClient, $this->httpClient_, null);
      $this->metricsCollector_ = new MetricsCollector($metricAdminClient);
      $httpClient->setMetricsCollector($this->metricsCollector_);
      $this->metricsCollector_->start();
      $thriftProtocol = $protocolClass->newInstanceArgs(array('trans' => $httpClient));
      $this->httpClient_ = $httpClient;
    }
    return new $clientClass($thriftProtocol, $thriftProtocol);
  }

  private function userAgent()
  {
    return "PHP-SDK/" . $this->version() . " PHP/" . phpversion();
  }

  private function version()
  {
    $v = $this->version_;
    return "{$v->major}.{$v->minor}.{$v->patch}";
  }
}

class RetryableClient
{
  private $maxRetry_;
  private $client_;
  private $httpClient_;
  private $metricsCollector_;

  public function __construct($client, $httpClient, $metricsCollector, $maxRetry = 1)
  {
    $this->client_ = $client;
    $this->httpClient_ = $httpClient;
    $this->maxRetry_ = $maxRetry;
    $this->metricsCollector_ = $metricsCollector;
  }

  public function __call($name, $arguments)
  {
    $method = new \ReflectionMethod($this->client_, $name);
    $queryString = 'type=' . $name;
    $this->httpClient_->setQueryString($queryString);
    $retry = 0;
    while (true) {
      $ex = null;
      $requestMetrics = new RequestMetrics();
      try {
        if ($this->metricsCollector_ != null) {
          $requestMetrics->setQueryString($queryString);
          $requestMetrics->startEvent(RequestMetrics::EXECUTION_TIME);
        }
        $result = $method->invokeArgs($this->client_, $arguments);
        $this->doCollectMetrics($requestMetrics);
        return $result;
      } catch (SdsException $e) {
        $ex = $e;
        $this->doCollectMetrics($requestMetrics);
      } catch (ServiceException $se) {
        $ex = SdsException::createServiceException("service",
            $se->errorCode,
            $se->errorMessage,
            $se->details,
            $se->callId,
            $se->requestId);
        $this->doCollectMetrics($requestMetrics);
      }
      $sleepMs = $this->backoffTime($ex->errorCode);
      if ($retry >= $this->maxRetry_ || $sleepMs < 0) {
        throw $ex;
      }
      usleep(1000 * ($sleepMs << $retry));
      $retry++;
    }

    return null;
  }

  private function doCollectMetrics($requestMetrics)
  {
    if ($this->metricsCollector_ != null) {
      $requestMetrics->endEvent(RequestMetrics::EXECUTION_TIME);
      $this->metricsCollector_->collect($requestMetrics);
    }
  }

  private function backoffTime($errorCode)
  {
    $backoffConf = Constant::get('ERROR_BACKOFF');
    if (array_key_exists($errorCode, $backoffConf)) {
      return $backoffConf[$errorCode];
    } else {
      return -1;
    }
  }
}
