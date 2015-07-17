<?php

namespace RPC\Auth;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

class Signer {

  static $s_sub_resources;

  /**
   * Sign the specified http request.
   *
   * @param string $http_method  The http request method
   * @param string $uri The uri string
   * @param array $http_headers The http request headers
   * @param string $access_secret The user's app secret
   * @param string $algorithm   The sign algorithm
   * @return string The signed result
   */
  public static function sign($http_method, $uri, $http_headers, $access_secret,
                              $algorithm) {
    $supported_algo = hash_algos();
    if (!array_search($algorithm, $supported_algo)) {
      throw new \RuntimeException(
          "Unsupported Hmac algorithm: " . $algorithm);
    }

    $string_to_sign = self::constructStringToSign($http_method, $uri, $http_headers);
    $result = hash_hmac($algorithm, $string_to_sign, $access_secret, true);
    return $result;
  }

  /**
   * @param string $http_method
   * @param string $uri
   * @param array $http_headers
   * @param string $access_secret
   * @param string $algorithm
   * @return string
   */
  public static function signToBase64($http_method, $uri, $http_headers,
                                      $access_secret, $algorithm) {
    $sign_result = self::sign($http_method, $uri, $http_headers,
        $access_secret, $algorithm);
    $encoded_result = base64_encode($sign_result);
    return $encoded_result;
  }

  static function constructStringToSign($http_method, $uri, $http_headers) {
    $result = "";
    $result .= $http_method . "\n";
    $result .= self::getHeaderValue($http_headers, 'content-md5') . "\n";
    $result .= self::getHeaderValue($http_headers, 'Content-Type') . "\n";
    $result .= self::getHeaderValue($http_headers, 'date') . "\n";
    $result .= self::canonicalizeXiaomiHeaders($http_headers);
    $result .= self::canonicalizeResource($uri);
    return $result;
  }

  static function canonicalizeXiaomiHeaders($headers) {
    if ($headers == NULL || empty($headers)) {
      return "";
    }

    // 1. Sort the header and merge the values
    $canonicalizedHeaders = array();
    foreach ($headers as $key => $value) {
      $key = strtolower($key);
      if (self::stringStartsWith($key, Constant::get('XIAOMI_HEADER_PREFIX'))) {
        if (is_array($value)) {
          $canonicalizedHeaders[$key] = join(",", $value);
        } else {
          $canonicalizedHeaders[$key] = $value;
        }
      }
    }
    ksort($canonicalizedHeaders);

    // 2. TODO(wuzesheng) Unfold multiple lines long header

    // 3. Generate the canonicalized result
    $result = "";
    foreach ($canonicalizedHeaders as $key => $value) {
      $result .= $key . ":" . $value . "\n";
    }
    return $result;
  }

  static function canonicalizeResource($uri) {
    $result = "";
    $result .= parse_url($uri, PHP_URL_PATH);

    // 1. Parse and sort subresource
    $sorted_params = array();
    $query = parse_url($uri, PHP_URL_QUERY);
    $params = array();
    parse_str($query, $params);
    foreach ($params as $key => $value) {
      if (self::$s_sub_resources != null) {
        if (array_search($key, self::$s_sub_resources) !== false) {
          $sorted_params[$key] = $value;
        }
      }
    }
    ksort($sorted_params);

    // 2. Generate the canonicalized result
    if (!empty($sorted_params)) {
      $result .= "?";
      $first = true;
      foreach ($sorted_params as $key => $value) {
        if ($first) {
          $first = false;
          $result .= $key;
        } else {
          $result .= "&" . $key;
        }

        if (!empty($value)) {
          $result .= "=" . $value;
        }
      }
    }
    return $result;
  }

  static function getHeaderValue($headers, $name) {
    if ($headers != NULL && array_key_exists($name, $headers)) {
      if (is_array($headers[$name])) {
        return $headers[$name][0];
      } else {
        return $headers[$name];
      }
    }
    return "";
  }

  static function stringStartsWith($haystack, $needle) {
    return $needle == "" || strpos($haystack, $needle) === 0;
  }
}

