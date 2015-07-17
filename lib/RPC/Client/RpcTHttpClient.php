<?php
namespace RPC\Client;

use RPC\Auth\Constant;
use RPC\Auth\Credential;
use RPC\Auth\Signer;
use RPC\Common\ThriftProtocol;
use RPC\Errors\HttpStatusCode;
use Thrift\Exception\TTransportException;
use Thrift\Factory\TStringFuncFactory;
use Thrift\Transport\THttpClient;

/*
 * Thrift HTTP transport for PRC service
 * @package RPC.Client
 */

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

class RpcTHttpClient extends THttpClient {
  const DATE_FORMAT = 'D, d M Y H:i:s \G\M\T';
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
   * @param string $credential
   * @param int $url
   * @param string $timeout
   * @param string $connTimeout
   * @param int $protocol
   * @param bool $retryIfOperationTimeout
   * @param bool $verbose
   */

  public function __construct($credential, $url, $timeout, $connTimeout,
      $protocol = ThriftProtocol::TCOMPACT, $retryIfOperationTimeout = false,
      $verbose = false) {
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
  public function setTimeoutSecs($timeout) {
    $this->timeout_ = $timeout;
  }

  /**
   * Set conn timeout
   *
   * @param float $connTimeout
   */
  public function setConnTimeoutSecs($connTimeout) {
    $this->connTimeout_ = $connTimeout;
  }

  /**
   * Set thrift protocol
   *
   * @param enum $protocol
   */
  public function setProtocol($protocol) {
    $this->protocol_ = $protocol;
  }

  /**
   * Set query string
   *
   * @param string $queryString
   */
  public function setQueryString($queryString) {
    $this->queryString_ = $queryString;
  }

  public function setSupportAccountKey($supportAccountKey) {
    $this->supportAccountKey_ = $supportAccountKey;
  }

  /**
   * Whether this transport is open.
   *
   * @return boolean true if open
   */
  public function isOpen() {
    return true;
  }

  /**
   * Open the transport for reading/writing
   *
   * @throws TTransportException if cannot open
   */
  public function open() {
  }

  /**
   * Close the transport.
   */
  public function close() {
  }

  /**
   * Read some data into the array.
   *
   * @param int $len How much to read
   * @return string The data that has been read
   * @throws TTransportException if cannot read any more data
   */
  public function read($len) {
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
  public function write($buf) {
    $this->buf_ .= $buf;
  }

  /**
   * Opens and sends the actual request over the HTTP connection
   *
   * @throws TTransportException if a writing error occurs
   */
  public function flush() {
    $headerMaps = \RPC\Common\Constant::get('THRIFT_HEADER_MAP');
    $defaultHeaders = array('Host' => $this->host_,
        'Accept' => $headerMaps[$this->protocol_],
        'User-Agent' => 'PHP/THttpClient',
        'Content-Type' => $headerMaps[$this->protocol_],
        'Content-Length' => TStringFuncFactory::create()->strlen($this->buf_));
    $headers = array();
    foreach (array_merge($defaultHeaders, $this->headers_,
        $this->getAuthenticationHeaders($this->url_, $defaultHeaders))
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
        throw new TTransportException("cURL error: " . $errmsg . ", errno $errno");
      }
      curl_close($ch);
      break;
    }

    $response = $this->parseResponse($raw);
    if (empty($response)) {
      throw new TTransportException("Unrecognized HTTP response: " . $raw);
    }
    if ($response['code'] == HttpStatusCode::CLOCK_TOO_SKEWED) {
      $now = new \DateTime();
      $serverTime = new \DateTime();
      $serverTime->setTimestamp($response['timestamp']);
      $this->clockOffset_ = $now->diff($serverTime);
    }
    if ($response['code'] != 200 &&
        strpos($response['content-type'], "application/x-thrift") !== 0) {
      throw new TTransportException($response['message'] . $response['body']);
    } else {
      $this->readOffset_ = 0;
      $this->response_ = $response['body'];
    }
  }

  private function retriableCurlError($errno) {
    if ($errno == CURLOPT_CONNECTTIMEOUT ||
        $errno == CURLE_COULDNT_CONNECT ||
        $errno == CURLE_OPERATION_TIMEOUTED && $this->retryIfOperationTimeout_) {
      return true;
    }
    return false;
  }

  private function getAuthenticationHeaders($uri, $default_headers) {
    if (!isset($this->credential_)) {
      return array();
    }
    $headers = array();
    $headers[Constant::get('HK_HOST')] = $this->host_;
    $now = new \DateTime();
    if (isset($this->clockOffset_)) {
      $now->add($this->clockOffset_); // adjust the local clock
    }
    $headers[Constant::get('HK_TIMESTAMP')] = $now->format('U');
    $headers[Constant::get('MI_DATE')] = gmdate(self::DATE_FORMAT, time());
    $headers[Constant::get('HK_CONTENT_MD5')] = md5($this->buf_);

    $signature = Signer::signToBase64("POST", $uri, array_merge($headers, $default_headers),
        $this->credential_->secretKey, "sha1");
    $authHeader = "Galaxy-V2 ".$this->credential_->secretKeyId.":".$signature;
    $headers[Constant::get('HK_AUTHORIZATION')] = $authHeader;

    return $headers;
  }

  private function parseResponse($raw) {
    if ($this->verbose_) {
      error_log("Get http response:\n" . $raw . "\n");
    }
    list($header, $body) = explode("\r\n\r\n", $raw, 2);
    $headers = explode("\r\n", $header);
    strtok($headers[0], " "); // skip http version
    $code = strtok(" ");
    $message = strtok("\r\n");
    $timestamp = null;
    $contentType = null;
    foreach ($headers as $h) {
      $parts = explode(":", $h);
      if (sizeof($parts) != 2) {
        continue;
      }
      list($key, $value) = $parts;
      if (trim($key) === Constant::get('HK_TIMESTAMP')) {
        $timestamp = intval(trim($value));
      }
      if (trim($key) === "Content-Type") {
        $contentType = trim($value);
      }
    }

    return array(
        "code" => intval($code),
        "message" => $message,
        "timestamp" => $timestamp,
        "body" => $body,
        "content-type" => $contentType
    );
  }

  public function addHeaders($headers) {
    $this->headers_ = array_merge($this->headers_, $headers);
  }

  private function generateRandomId() {
    return sprintf('%04x%04x', mt_rand(0, 0xffff), mt_rand(0, 0xffff));
  }
}