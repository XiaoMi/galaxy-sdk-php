<?php
/**
 * User: heliangliang
 * Date: 5/13/14
 * Time: 7:19 PM
 */

namespace SDS\Client;

use SDS\Errors\ErrorCode;
use SDS\Errors\HttpStatusCode;

class SdsException extends \Exception
{
  /**
   * @var int http status code
   */
  public $httpStatusCode;
  /**
   * @var string error code from server
   */
  public $errorCode;
  /**
   * @var string error message from server
   */
  public $errorMessage;
  /**
   * @var string detailed error message from server
   */
  public $details;
  /**
   * @var string call id identifies the call, assigned from server side
   */
  public $callId;
  /**
   * @var string error type ['transport', 'service']
   */
  public $type;

  public function __construct($arr)
  {
    $this->type = $arr['type'];
    $this->httpStatusCode = $arr['httpStatusCode'];
    $this->errorCode = $arr['errorCode'];
    $this->errorMessage = $arr['errorMessage'];
    $this->details = $arr['details'];
    $this->callId = $arr['callId'];

    parent::__construct($this->getErrorString());
  }

  private function getErrorString()
  {
    $errorCode = ErrorCode::$__names[$this->errorCode];
    switch ($this->type) {
      case "transport":
        return "HTTP transport error [http status code: " . $this->httpStatusCode .
        ", error code: " . $errorCode . ", error message: " . $this->errorMessage .
        "]";
      case "service":
        return "Service error [error code: " . $errorCode .
        ", error message: " . $this->errorMessage . ", details: " . $this->details .
        ", call id: " . $this->callId . "]";
      default:
        return "Unknown exception";
    }
  }

  public static function createServiceException($type, $errorCode, $errorMessage, $details, $callId)
  {
    return new SdsException(array(
      "type" => $type,
      "errorCode" => $errorCode,
      "errorMessage" => $errorMessage,
      "details" => $details,
      "callId" => $callId,
      "httpStatusCode" => 200
    ));
  }

  public static function createTransportException($httpStatusCode, $errorMessage)
  {
    switch ($httpStatusCode) {
      case HttpStatusCode::INVALID_AUTH:
        $errorCode = ErrorCode::INVALID_AUTH;
        break;
      case HttpStatusCode::CLOCK_TOO_SKEWED:
        $errorCode = ErrorCode::CLOCK_TOO_SKEWED;
        break;
      case HttpStatusCode::REQUEST_TOO_LARGE:
        $errorCode = ErrorCode::REQUEST_TOO_LARGE;
        break;
      case HttpStatusCode::BAD_REQUEST:
        $errorCode = ErrorCode::BAD_REQUEST;
        break;
      case HttpStatusCode::INTERNAL_ERROR:
        $errorCode = ErrorCode::INTERNAL_ERROR;
        break;
      default:
        $errorCode = ErrorCode::UNKNOWN;
    }
    return new SdsException(array(
      "type" => "transport",
      "httpStatusCode" => $httpStatusCode,
      "errorCode" => $errorCode,
      "errorMessage" => $errorMessage,
      "details" => "",
      "callId" => ""
    ));
  }
} 