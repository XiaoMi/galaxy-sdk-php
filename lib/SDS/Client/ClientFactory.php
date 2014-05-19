<?php
/**
 * User: heliangliang
 * Date: 5/13/14
 * Time: 9:00 PM
 */

namespace SDS\Client;

use SDS\Common\Version;
use SDS\Errors\Constant;
use SDS\Errors\ServiceException;
use Thrift\Protocol\TJSONProtocol;

class ClientFactory
{
  private $credential_;
  private $version_;
  private $verbose_;
  protected $retryIfOperationTimeout_;

  /**
   * @param \SDS\Auth\Credential $credential
   * @param bool $retryIfOperationTimeout
   * Do automatic retry when curl reports CURLE_OPERATION_TIMEOUTED error,
   * note that the request may has been sent to the server successfully.
   * Don't set this when the operation is not idempotent.
   * @param bool $verbose
   */
  public function __construct($credential, $retryIfOperationTimeout = false, $verbose = false)
  {
    $this->credential_ = $credential;
    $this->version_ = new Version();
    $this->verbose_ = $verbose;
    $this->retryIfOperationTimeout_ = $retryIfOperationTimeout;
  }

  /**
   * @return \SDS\Auth\AuthServiceClient OAuth token generation service client
   */
  public function newDefaultAuthClient()
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
      Constant::get('TABLE_AUTH_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newAuthClient($url, $timeout, $connTimeout);
  }

  /**
   * @param $url string the authentication endpoint url
   * @return \SDS\Auth\AuthServiceClient OAuth token generation service client
   */
  public function newAuthClient($url, $timeout, $connTimeout)
  {
    $client = $this->getClient('SDS\Auth\AuthServiceClient', $url, $timeout, $connTimeout);
    return new RetryableClient($client);
  }

  /**
   * @return \SDS\Admin\AdminServiceClient
   */
  public function newDefaultAdminClient()
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
      Constant::get('ADMIN_SERVICE_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newAdminClient($url, $timeout, $connTimeout);
  }

  /**
   * @param $url string the administration endpoint url
   * @return \SDS\Admin\AdminServiceClient
   */
  public function newAdminClient($url, $timeout, $connTimeout)
  {
    $client = $this->getClient('SDS\Admin\AdminServiceClient', $url, $timeout, $connTimeout);
    return new RetryableClient($client);
  }

  /**
   * @return \SDS\Table\TableServiceClient
   */
  public function newDefaultTableClient()
  {
    $url = Constant::get('DEFAULT_SERVICE_ENDPOINT') .
      Constant::get('TABLE_SERVICE_PATH');
    $timeout = Constant::get('DEFAULT_CLIENT_TIMEOUT');
    $connTimeout = Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT');
    return $this->newTableClient($url, $timeout, $connTimeout);
  }

  /**
   * @param $url string the table access endpoint url
   * @return \SDS\Table\TableServiceClient
   */
  public function newTableClient($url, $timeout, $connTimeout)
  {
    $client = $this->getClient('SDS\Table\TableServiceClient', $url, $timeout, $connTimeout);
    return new RetryableClient($client);
  }

  protected function getClient($clientClass, $url, $timeout, $connTimeout)
  {
    $parts = parse_url($url);
    if (!isset($parts['port'])) {
      if ($parts['scheme'] === 'https') {
        $parts['port'] = 443;
      } else {
        $parts['port'] = 80;
      }
    }

    $httpClient = new SdsTHttpClient($this->credential_, $url, $timeout,
        $connTimeout, $this->retryIfOperationTimeout_, $this->verbose_);
    $httpClient->addHeaders(array('User-Agent' => $this->userAgent()));
    $thriftProtocol = new TJSONProtocol($httpClient);

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

  public function __construct($client, $maxRetry = 1)
  {
    $this->client_ = $client;
    $this->maxRetry_ = $maxRetry;
  }

  public function __call($name, $arguments)
  {
    $method = new \ReflectionMethod($this->client_, $name);
    $retry = 0;
    while (true) {
      $ex = null;
      try {
        return $method->invokeArgs($this->client_, $arguments);
      } catch (SdsException $e) {
        $ex = $e;
      } catch (ServiceException $se) {
        $ex = SdsException::createServiceException("service",
          $se->errorCode,
          $se->errorMessage,
          $se->details,
          $se->callId);
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

  private function backoffTime($errorCode)
  {
    $backoffConf = Constant::get('ERROR_AUTO_BACKOFF');
    if(array_key_exists($errorCode, $backoffConf)) {
      return $backoffConf[$errorCode];
    } else {
      return -1;
    }
  }
}
