<?php

/**
 * User: heliangliang
 * Date: 5/14/14
 * Time: 9:54 PM
 */

namespace SDS\Client;

class TableScanner
{
  /**
   * @var \SDS\Table\TableServiceClient
   */
  public $tableClient_;
  /**
   * @var \SDS\Table\ScanRequest
   */
  public $scanRequest_;

  public function __construct($tableClient, $scanRequest)
  {
    $this->tableClient_ = $tableClient;
    $this->scanRequest_ = $scanRequest;
  }

  /**
   * @return RecordIterator record iterator for the scan
   */
  public function iterator()
  {
    return new RecordIterator($this->tableClient_, $this->scanRequest_);
  }
}

class RecordIterator implements \Iterator
{
  /**
   * @var \SDS\Table\TableServiceClient
   */
  public $tableClient_;
  /**
   * @var \SDS\Table\ScanRequest
   */
  public $scanRequest_;

  private $startKey_;

  /**
   * @var ArrayIterator
   */
  private $iter_;
  /**
   * @var int counter
   */
  private $pos_;

  private $finished_;

  private $retryTime_;

  private $baseWaitTime_;

  public function __construct($tableClient, $scanRequest)
  {
    $this->tableClient_ = $tableClient;
    $this->scanRequest_ = $scanRequest;
    $this->startKey_ = $scanRequest->startKey;
    $this->iter_ = null;
    $this->pos_ = null;
    $this->finished_ = false;
    $this->retryTime_ = 0;
    $this->baseWaitTime_ = 500 * 1000; // 0.5s
  }

  public function current()
  {
    return $this->iter_->current();
  }

  public function key()
  {
    return $this->pos_;
  }

  public function next()
  {
    ++$this->pos_;
    $this->iter_->next();
  }

  public function rewind()
  {
    $this->iter_ = null;
    $this->pos_ = 0;
    $this->finished_ = false;
    $this->scanRequest_->startKey = $this->startKey_;
  }

  public function valid()
  {
    if (!isset($this->iter_) || !$this->iter_->valid()) {
      if ($this->finished_) {
        return false;
      } else {
        // call scan
        if ($this->retryTime_ > 0) {
            usleep($this->baseWaitTime_ << ($this->retryTime_ - 1));
        }
        $result = $this->tableClient_->scan($this->scanRequest_);
        if (empty($result->nextStartKey)) {
          $this->finished_ = true;
        } else {
           if (count($result->records) == $this->scanRequest_->limit) {
               $this->retryTime_ = 0;
           } else {
               $this->retryTime_++;
           }
        }
        $this->scanRequest_->startKey = $result->nextStartKey;
        $this->iter_ = new \ArrayIterator($result->records);
        $this->iter_->rewind();
      }
    }
    return $this->iter_->valid();
  }
}
