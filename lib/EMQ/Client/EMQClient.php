<?php

namespace EMQ\Client;

use EMQ\Common\GalaxyEmqServiceException;
use EMQ\Range\Constant;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */
class EMQClient {
  private $client_;

  /**
   * @param \RPC\Client\RetryableClient $client
   */
  public function __construct($client) {
    $this->client_ = $client;
  }

  public function __call($name, $arguments) {
    $this->checkArgument($name, $arguments);
    return $this->client_->__call($name, $arguments);
  }

  public function checkArgument($name, $arguments) {
    switch ($name) {
      case 'createQueue':
        $queueName = $arguments[0]->queueName;
        self::checkNotEmpty($queueName, "queue name");
        if (substr_count($queueName, "/") != 0) {
          throw new GalaxyEmqServiceException(array('errMsg' => 'Invalid Queue Name',
              'details' => 'invalid characters in queue name'));
        }
        self::validateQueueAttribute($arguments[0]->queueAttribute);
        self::validateQueueQuota($arguments[0]->queueQuota);
        break;
      case 'deleteQueue':
      case 'purgeQueue':
      case 'getQueueInfo':
      case 'queryPermission':
      case 'listPermission':
      case 'listTag':
        self::validateQueueName($arguments[0]->queueName);
        break;
      case 'setQueueAttributes':
        self::validateQueueName($arguments[0]->queueName);
        self::validateQueueAttribute($arguments[0]->queueAttribute);
        break;
      case 'setQueueQuota':
        self::validateQueueName($arguments[0]->queueName);
        self::validateQueueQuota($arguments[0]->queueQuota);
        break;
      case 'listQueue':
        self::validateQueueNamePrefix($arguments[0]->queueNamePrefix);
        break;
      case 'setPermission':
      case 'revokePermission':
      case 'queryPermissionForId':
        self::validateQueueName($arguments[0]->queueName);
        self::checkNotEmpty($arguments[0]->developerId, "developerId");
        break;
      case 'sendMessageBatch':
        self::validateQueueName($arguments[0]->queueName);
        $entryList = $arguments[0]->sendMessageBatchRequestEntryList;
        self::checkNotEmpty($entryList, "send message list");
        $idList = array();
        foreach ($entryList as $entry) {
          $entryId = $entry->entryId;
          self::checkNotEmpty($entryId, "entityId");
          if (in_array($entryId, $idList, true)) {
            throw new GalaxyEmqServiceException(array(
                'errMsg' => 'Not Unique EntityId',
                'details' => "Duplicate entryId:" . $entryId));
          }
          $idList[] = $entryId;
          self::checkSendEntry($entry);
        }
        break;
      case 'createTag':
        self::validateQueueName($arguments[0]->queueName);
        self::validateTagName($arguments[0]->tagName);
        self::checkParameterRange("tagReadQPS", $arguments[0]->readQPSQuota,
            Constant::get('GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL'),
            Constant::get('GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL'));
        if (!is_null($arguments[0]->userAttributes)) {
          self::validateUserAttributes($arguments[0]->userAttributes);
        }
        if (!is_null($arguments[0]->attributeName)) {
          self::checkNotEmpty($arguments[0]->attributeName, "attributeName");
          self::checkMessageAttribute($arguments[0]->attributeValue, true);
        }
        break;
      case 'getTagInfo':
        self::validateQueueName($arguments[0]->queueName);
        if (!is_null($arguments[0]->tagName)) {
          self::validateTagName($arguments[0]->tagName);
        }
        break;
      case 'deleteTag':
        self::validateQueueName($arguments[0]->queueName);
        self::validateTagName($arguments[0]->tagName);
        break;
      case 'sendMessage':
        self::validateQueueName($arguments[0]->queueName);
        self::checkSendEntry($arguments[0]);
        break;
      case 'receiveMessage':
        self::validateQueueName($arguments[0]->queueName);
        self::checkParameterRange("receiveMessageMaximumNumber",
            $arguments[0]->maxReceiveMessageNumber,
            Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL'),
            Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL'));
        self::checkParameterRange("receiveMessageMaximumWaitSeconds",
            $arguments[0]->maxReceiveMessageWaitSeconds,
            Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL'),
            Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL'));
        if (!is_null($arguments[0]->attributeName)) {
          self::checkNotEmpty($arguments[0]->attributeName, "attributeName");
          self::checkMessageAttribute($arguments[0]->attributeValue, true);
        }
        break;
      case 'changeMessageVisibilitySecondsBatch':
        self::validateQueueName($arguments[0]->queueName);
        $entryList = $arguments[0]->changeMessageVisibilityRequestEntryList;
        self::checkIsSet($entryList, "change message visibility list");
        $idList = array();
        foreach ($entryList as $entry) {
          $receiptHandle = $entry->receiptHandle;
          self::checkNotEmpty($receiptHandle, "receipt handle");
          if (in_array($receiptHandle, $idList, true)) {
            throw new GalaxyEmqServiceException(array(
                'errMsg' => 'Not Unique ReceiptHandle',
                'details' => "Duplicate receiptHandle:" . $receiptHandle));
          }
          $idList[] = $receiptHandle;
          self::checkNotEmpty($entry->invisibilitySeconds, "invisibility seconds");
          self::checkParameterRange("invisibilitySeconds",
              $entry->invisibilitySeconds, 0,
              Constant::get('GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL'));
        }
        break;
      case 'changeMessageVisibilitySeconds':
        self::validateQueueName($arguments[0]->queueName);
        self::checkNotEmpty($arguments[0]->receiptHandle, "receipt handle");
        self::checkIsSet($arguments[0]->invisibilitySeconds,
            "invisibility seconds");
        self::checkParameterRange("invisibilitySeconds",
            $arguments[0]->invisibilitySeconds, 0,
            Constant::get('GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL'));
        break;
      case 'deleteMessageBatch':
        self::validateQueueName($arguments[0]->queueName);
        $entryList = $arguments[0]->deleteMessageBatchRequestEntryList;
        self::checkNotEmpty($entryList, "delete message list");
        $idList = array();
        foreach ($entryList as $entry) {
          $receiptHandle = $entry->receiptHandle;
          self::checkNotEmpty($receiptHandle, "receipt handle");
          if (in_array($receiptHandle, $idList, true)) {
            throw new GalaxyEmqServiceException(array(
                'errMsg' => 'Not Unique ReceiptHandle',
                'details' => "Duplicate receiptHandle:" . $receiptHandle));
          }
          $idList[] = $receiptHandle;
        }
        break;
      case 'deleteMessage':
        self::validateQueueName($arguments[0]->queueName);
        self::checkNotEmpty($arguments[0]->receiptHandle, "receipt handle");
        break;
    }
  }

  /**
   * @param \EMQ\Message\SendMessageBatchRequestEntry,
   *        \EMQ\Message\SendMessageRequest $entry
   * @throws GalaxyEmqServiceException
   */
  public static function checkSendEntry($entry) {
    self::checkNotEmpty($entry->messageBody, "message body");
    self::checkParameterRange("delaySeconds", $entry->delaySeconds,
        Constant::get('GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL'));
    self::checkParameterRange("invisibilitySeconds", $entry->invisibilitySeconds,
        Constant::get('GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL'));
    if (!is_null($entry->messageAttributes)) {
      foreach ($entry->messageAttributes as $name => $attribute) {
        self::checkNotEmpty($name, "attribute name");
        self::checkMessageAttribute($attribute, false);
      }
    }
  }

  public static function checkMessageAttribute($attribute, $allow_empty) {
    if (is_null($attribute)) {
      throw new GalaxyEmqServiceException(array(
          'errMsg' => 'Message attribute is null'));
    }
    if (stripos($attribute->type, 'string') === 0) {
      if (is_null($attribute->stringValue)) {
        throw new GalaxyEmqServiceException(array(
            'errMsg' => 'Invalid user-defined attributes',
            'details' => 'stringValue cannot be null when type is STRING'));
      }
    } else if (stripos($attribute->type, 'binary') === 0) {
      if (is_null($attribute->binaryValue)) {
        throw new GalaxyEmqServiceException(array(
            'errMsg' => 'Invalid user-defined attributes',
            'details' => 'binaryValue cannot be null when type is BINARY'));
      }
    } else if ($allow_empty && strcasecmp($attribute->type, 'empty') === 0) {
      return;
    } else {
      throw new GalaxyEmqServiceException(array(
          'errMsg' => 'Invalid user-defined attributes',
          'details' => "Attribute type must start with \"STRING\" or \"BINARY\""));
    }
    foreach (str_split($attribute->type) as $c) {
      if (!ctype_alnum($c) && $c !== '.') {
        throw new GalaxyEmqServiceException(array(
            'errMsg' => 'Invalid user-defined attributes',
            'details' => "Invalid character '" . $c . "' in attribute type"));
      }
    }
  }

  /**
   * @param \EMQ\Queue\QueueAttribute $attribute
   */
  public static function validateQueueAttribute($attribute) {
    if (is_null($attribute)) {
      return;
    }
    self::checkParameterRange("delaySeconds", $attribute->delaySeconds,
        Constant::get('GALAXY_EMQ_QUEUE_DELAY_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_DELAY_SECONDS_MAXIMAL'));

    self::checkParameterRange("invisibilitySeconds", $attribute->invisibilitySeconds,
        Constant::get('GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL'));

    self::checkParameterRange("receiveMessageWaitSeconds",
        $attribute->receiveMessageWaitSeconds,
        Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL'));

    self::checkParameterRange("receiveMessageMaximumNumber",
        $attribute->receiveMessageMaximumNumber,
        Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL'));

    self::checkParameterRange("messageRetentionSeconds",
        $attribute->messageRetentionSeconds,
        Constant::get('GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MAXIMAL'));

    self::checkParameterRange("messageMaximumBytes",
        $attribute->messageMaximumBytes,
        Constant::get('GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MAXIMAL'));

    self::checkParameterRange("partitionNumber", $attribute->partitionNumber,
        Constant::get('GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MAXIMAL'));

    if (!is_null($attribute->userAttributes)) {
      self::validateUserAttributes($attribute->userAttributes);
    }
  }

  public static function validateUserAttributes($attributes) {
    foreach ($attributes as $key => $value) {
      self::checkNotEmpty($key, "user attribute name");
      self::checkNotEmpty($value, "user attribute value for \"$key\"");
    }
  }

  /**
   * @param \EMQ\Queue\QueueQuota $queueQuota
   */
  public static function validateQueueQuota($queueQuota) {
    if (is_null($queueQuota)) {
      return;
    }
    if (is_null($queueQuota->spaceQuota)) {
      return;
    }
    $spaceQuota = $queueQuota->spaceQuota;
    self::checkParameterRange("spaceQuota", $spaceQuota->size,
        Constant::get('GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MAXIMAL'));
    if (is_null($queueQuota->throughput)) {
      return;
    }
    $throughput = $queueQuota->throughput;
    self::checkParameterRange("readQps", $throughput->readQps,
        Constant::get('GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL'));
    self::checkParameterRange("writeQps", $throughput->writeQps,
        Constant::get('GALAXY_EMQ_QUEUE_WRITE_QPS_MINIMAL'),
        Constant::get('GALAXY_EMQ_QUEUE_WRITE_QPS_MAXIMAL'));
  }

  public static function validateQueueName($queueName) {
    self::checkNotEmpty($queueName, "queue name");
    if (substr_count($queueName, '/') !== 1) {
      throw new GalaxyEmqServiceException(array('errMsg' => 'Invalid Queue Name',
          'details' => 'allow exactly one "/" in queue name:' . $queueName));
    }
  }

  public static function validateTagName($tagName) {
    self::checkNotEmpty($tagName, "tag name");
  }

  public static function validateQueueNamePrefix($queueNamePrefix) {
    if (substr_count($queueNamePrefix, '/') > 1) {
      throw new GalaxyEmqServiceException(
          array('errMsg' => 'Invalid queue name prefix',
              'details' => 'allow at most one "/" in queueNamePrefix:' . $queueNamePrefix));
    }
  }

  public static function checkParameterRange($parameter, $val, $minVal, $maxVal) {
    if (!is_null($val) && ($val < $minVal || $val > $maxVal)) {
      throw new GalaxyEmqServiceException(array('errMsg' => 'Parameter Out of Range',
          'details' => "$parameter:$val should in range [$minVal, $maxVal]"));
    }
  }

  public static function checkIsSet($obj, $name) {
    if (!isset($obj)) {
      throw new GalaxyEmqServiceException(array('errMsg' => "$name is not set"));
    }
  }

  public static function checkNotEmpty($obj, $name) {
    if (is_null($obj) || empty($obj)) {
      throw new GalaxyEmqServiceException(array('errMsg' => 'empty ' . $name));
    }
  }
}