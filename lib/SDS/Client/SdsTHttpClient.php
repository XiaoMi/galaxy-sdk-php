<?php
/**
 * User: heliangliang
 * Date: 5/13/14
 * Time: 9:12 PM
 */
namespace SDS\Client;

use SDS\Auth\Constant;
use SDS\Auth\Credential;
use SDS\Auth\HttpAuthorizationHeader;
use SDS\Auth\MacAlgorithm;
use SDS\Common\ThriftProtocol;
use SDS\Errors\HttpStatusCode;
use Thrift\Exception\TTransportException;
use Thrift\Factory\TStringFuncFactory;
use Thrift\Protocol\TJSONProtocol;
use Thrift\Transport\THttpClient;
use Thrift\Transport\TMemoryBuffer;

/*
 * Thrift HTTP transport for SDS service
 * @package SDS.Client
 */

class SdsTHttpClient extends THttpClient
{
  /**
   * Endpoint URL
   * @var string
   */
  protected $url_;
  /**
   * Endpoint host
   * @var string
   */
  protected $host_;
  /**
   * Buffer for the HTTP request data
   *
   * @var string
   */
  protected $buf_;
  /**
   * Response buffer
   * @var string
   */
  protected $response_;
  /**
   * Response buffer offset
   * @var int
   */
  protected $readOffset_;
  /**
   * Read timeout
   * @var float
   */
  protected $timeout_;
  /**
   * Connection timeout
   * @var float
   */
  protected $connTimeout_;
    /**
   * HTTP headers
   * @var array
   */
  protected $headers_;
  /**
   * Client credential
   * @var Credential
   */
  protected $credential_;
  /**
   * Do automatic retry when curl reports CURLE_OPERATION_TIMEOUTED error,
   * note that the request may has been sent to the server successfully
   * @var bool
   */
  protected $retryIfOperationTimeout_;
  /**
   * Enable verbose logging
   * @var bool
   */
  protected $verbose_;
  /**
   * Clock offset between server and client
   * @var \DateInterval
   */
  protected $clockOffset_;
  /**
   * Query string in url
   * @var string
   */
  protected $queryString_;

  protected $supportAccountKey_;

  protected $protocol_;
  /**
   * Make a new HTTP client.
   *
   * @param Credential $credential
   * @param string $url
   * @param bool $verbose
   */
  public function __construct($credential, $url, $timeout, $connTimeout, $protocol = ThriftProtocol::TBINARY,
                              $retryIfOperationTimeout = false, $verbose = false)
  {
    $parts = parse_url($url);
    $this->host_ = $this->getHostHeader($parts);
    $this->url_ = $url;
    $this->credential_ = $credential;
    $this->verbose_ = $verbose;
    $this->retryIfOperationTimeout_ = $retryIfOperationTimeout;
    $this->buf_ = '';
    $this->response_ = null;
    $this->timeout_ = $timeout;
    $this->connTimeout_ = $connTimeout;
    $this->protocol_ = $protocol;
    $this->headers_ = array();
    $this->clockOffset_ = null;
    $this->readOffset_ = 0;
    $this->queryString_ = '';
    $this->supportAccountKey_ = false;
  }

  private function getHostHeader($parts) {
    $scheme = $parts['scheme'];
    if (array_key_exists('port', $parts)) {
      $port = $parts['port'];
    } else {
      if ($scheme == 'http') {
        $port = 80;
      } else if ($scheme == 'https') {
        $port = 443;
      } else {
        die("unknown scheme: " . $scheme);
      }
    }
    $host = $parts['host'] .
      ($scheme == 'http' && $port == 80 ||
        $scheme == 'https' && $port == 443 ? '' : ':' . $port);
    return $host;
  }

  /**
   * Set read timeout
   *
   * @param float $timeout
   */
  public function setTimeoutSecs($timeout)
  {
    $this->timeout_ = $timeout;
  }

  /**
   * Set conn timeout
   *
   * @param float $connTimeout
   */
  public function setConnTimeoutSecs($connTimeout)
  {
    $this->connTimeout_ = $connTimeout;
  }

  /**
   * Set thrift protocol
   *
   * @param enum $protocol
   */
  public function setProtocol($protocol)
  {
    $this->protocol_ = $protocol;
  }

  /**
   * Set query string
   *
   * @param string $queryString
   */
  public function setQueryString($queryString)
  {
    $this->queryString_ = $queryString;
  }

  public function setSupportAccountKey($supportAccountKey)
  {
    $this->supportAccountKey_ = $supportAccountKey;
  }

  public function setMetricsCollector($metricsCollector){
    $this->metricsCollector_ = $metricsCollector;
  }

  /**
   * Whether this transport is open.
   *
   * @return boolean true if open
   */
  public function isOpen()
  {
    return true;
  }

  /**
   * Open the transport for reading/writing
   *
   * @throws TTransportException if cannot open
   */
  public function open()
  {
  }

  /**
   * Close the transport.
   */
  public function close()
  {
  }

  /**
   * Read some data into the array.
   *
   * @param int $len How much to read
   * @return string The data that has been read
   * @throws TTransportException if cannot read any more data
   */
  public function read($len)
  {
    $str = substr($this->response_, $this->readOffset_, $len);
    $this->readOffset_ += $len;
    return $str;
  }

  /**
   * Writes some data into the pending buffer
   *
   * @param string $buf The data to write
   * @throws TTransportException if writing fails
   */
  public function write($buf)
  {
    $this->buf_ .= $buf;
  }

  /**
   * Opens and sends the actual request over the HTTP connection
   *
   * @throws TTransportException if a writing error occurs
   */
  public function flush()
  {
    $headerMaps = \SDS\Common\Constant::get('THRIFT_HEADER_MAP');
    $defaultHeaders = array('Host' => $this->host_,
      'Accept' => $headerMaps[$this->protocol_],
      'User-Agent' => 'PHP/THttpClient',
      'Content-Type' => $headerMaps[$this->protocol_],
      'Content-Length' => TStringFuncFactory::create()->strlen($this->buf_));
    $headers = array();
    foreach (array_merge($defaultHeaders, $this->headers_, $this->getAuthenticationHeaders())
             as $key => $value) {
      $headers[] = "$key: $value";
    }
    $headers[] = "Expect:"; // fix Expect: 100-continue issue
    $uri = $this->url_ . '?id=' . $this->generateRandomId();
    if ($this->queryString_ != '') {
      $uri = $uri . '&' . $this->queryString_;
    }
    $ch = curl_init($uri);

    curl_setopt_array($ch, array(
      CURLOPT_RETURNTRANSFER => true,
      CURLOPT_URL => $uri,
      CURLOPT_HTTPHEADER => $headers,
      CURLOPT_POST => true,
      CURLOPT_HEADER => true,
      CURLOPT_POSTFIELDS => $this->buf_,
      CURLOPT_TIMEOUT => $this->timeout_,
      CURLOPT_CONNECTTIMEOUT => $this->connTimeout_,
      CURLOPT_VERBOSE => $this->verbose_
    ));

    if ($this->verbose_) {
      error_log("Send http request:\n" . $this->buf_ . "\n");
    }
    $retry = 0;
    while (true) {
      $raw = curl_exec($ch);
      $errno = curl_errno($ch);
      if ($this->retriableCurlError($errno) && $retry < 3) { // at most retry 2 times
        $retry++;
        continue;
      }
      $this->buf_ = '';
      if ($errno != CURLE_OK) {
        $errmsg = curl_error($ch);
        curl_close($ch);
        throw SdsException::createTransportException(0, "cURL error: " .
          $errmsg . ", errno $errno");
      }
      curl_close($ch);
      break;
    }

    $response = $this->parseResponse($raw);
    if (empty($response)) {
      throw SdsException::createTransportException(0, "Unrecognized HTTP response: " . $raw);
    }
    if ($response['code'] == HttpStatusCode::CLOCK_TOO_SKEWED) {
      $now = new \DateTime();
      $serverTime = new \DateTime();
      $serverTime->setTimestamp($response['timestamp']);
      $this->clockOffset_ = $now->diff($serverTime);
    }
    if ($response['code'] != 200) {
      throw SdsException::createTransportException($response['code'], $response['message']);
    } else {
      $this->readOffset_ = 0;
      $this->response_ = $response['body'];
    }
  }

  private function retriableCurlError($errno)
  {
    if ($errno == CURLOPT_CONNECTTIMEOUT ||
        $errno == CURLE_COULDNT_CONNECT ||
        $errno == CURLE_OPERATION_TIMEOUTED && $this->retryIfOperationTimeout_) {
      return true;
    }
    return false;
  }

  private function getAuthenticationHeaders()
  {
    if (!isset($this->credential_)) {
      return array();
    }
    $len = TStringFuncFactory::create()->strlen($this->buf_);
    if ($len > \SDS\Common\Constant::get('MAX_CONTENT_SIZE')) {
      throw SdsException::createTransportException(0, "Request too large, exceeds the max allowed size: " .
        \SDS\Common\Constant::get('MAX_CONTENT_SIZE'));
    }

    $headers = array();
    $headers[Constant::get('HK_HOST')] = $this->host_;
    $now = new \DateTime();
    if (isset($this->clockOffset_)) {
      $now->add($this->clockOffset_); // adjust the local clock
    }
    $headers[Constant::get('HK_TIMESTAMP')] = $now->format('U');
    $headers[Constant::get('HK_CONTENT_MD5')] = md5($this->buf_);

    $data = implode("\n", array_values($headers));
    $signature = hash_hmac("sha1", $data, $this->credential_->secretKey);

    $authHeader = new HttpAuthorizationHeader(
      array(
        "algorithm" => MacAlgorithm::HmacSHA1,
        "signedHeaders" => array_keys($headers),
        "userType" => $this->credential_->type,
        "secretKeyId" => $this->credential_->secretKeyId,
        "signature" => $signature,
        "supportAccountKey" => $this->supportAccountKey_
      )
    );

    $mb = new TMemoryBuffer();
    $protocol = new TJSONProtocol($mb);
    $authHeader->write($protocol);
    $headers[Constant::get('HK_AUTHORIZATION')] = $mb->getBuffer();

    return $headers;
  }

  private function parseResponse($raw)
  {
    if ($this->verbose_) {
      error_log("Get http response:\n" . $raw . "\n");
    }
    list($header, $body) = explode("\r\n\r\n", $raw, 2);
    $headers = explode("\r\n", $header);
    strtok($headers[0], " "); // skip http version
    $code = strtok(" ");
    $message = strtok("\r\n");
    $timestamp = null;
    foreach ($headers as $h) {
      $parts = explode(":", $h);
      if (sizeof($parts) != 2) {
        continue;
      }
      list($key, $value) = $parts;
      if (trim($key) === Constant::get('HK_TIMESTAMP')) {
        $timestamp = intval(trim($value));
        break;
      }
    }

    return array(
      "code" => intval($code),
      "message" => $message,
      "timestamp" => $timestamp,
      "body" => $body
    );
  }

  public function addHeaders($headers)
  {
    $this->headers_ = array_merge($this->headers_, $headers);
  }

  private function generateRandomId()
  {
    return sprintf('%04x%04x', mt_rand(0, 0xffff), mt_rand(0, 0xffff));
  }
}
