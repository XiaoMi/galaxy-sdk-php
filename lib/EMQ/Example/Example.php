<?php

namespace EMQ\Example;

use EMQ\Client\EMQClientFactory;
use EMQ\Common\GalaxyEmqServiceException;
use EMQ\Message\ChangeMessageVisibilityRequest;
use EMQ\Message\DeleteMessageBatchRequest;
use EMQ\Message\DeleteMessageBatchRequestEntry;
use EMQ\Message\ReceiveMessageRequest;
use EMQ\Message\SendMessageRequest;
use EMQ\Queue\CreateQueueRequest;
use EMQ\Queue\DeleteQueueRequest;
use EMQ\Queue\CreateTagRequest;
use EMQ\Queue\DeleteTagRequest;
use EMQ\Queue\QueueQuota;
use EMQ\Queue\SetQueueQuotaRequest;
use EMQ\Queue\SpaceQuota;
use EMQ\Queue\Throughput;
use RPC\Auth\Credential;
use RPC\Auth\UserType;
use Thrift\Exception\TException;

require_once dirname(__DIR__) . "/autoload.php";

// Set your AppKey and AppSecret
$appKey = "";
$appSecret = "";
$name = "testPHPClient";

$credential = new Credential (
    array(
        "type" => UserType::APP_SECRET,
        "secretKeyId" => $appKey,
        "secretKey" => $appSecret
    )
);

$clientFactory = new EMQClientFactory($credential);

$queueClient = $clientFactory->newDefaultQueueClient();
$messageClient = $clientFactory->newDefaultMessageClient();


try {
  $createQueueRequest = new CreateQueueRequest(array(
      'queueName' => $name,
      'queueQuota' => new QueueQuota(array(
          'spaceQuota' => new SpaceQuota(array('size' => 100)),
          'throughput' => new Throughput(array('readQps' => 100, 'writeQps' => 100))
      ))));
  $createQueueResponse = $queueClient->createQueue($createQueueRequest);
  print_r($createQueueResponse);
  $queueName = $createQueueResponse->queueName;

  $tagName = "tagTest";
  $createTagRequest = new CreateTagRequest(array(
    'queueName' => $queueName,
    'tagName' => $tagName,
  ));
  print_r($queueClient->createTag($createTagRequest));

  $messageBody = "EMQExample";
  $sendMessageRequest = new SendMessageRequest();
  $sendMessageRequest->queueName = $queueName;
  $sendMessageRequest->messageBody = $messageBody;
  $sendMessageResponse = $messageClient->sendMessage($sendMessageRequest);
  echo "Send:\n MessageBody: $messageBody " .
      "MessageId: $sendMessageResponse->messageID\n\n";

  $receiveMessageRequest = new ReceiveMessageRequest();
  $receiveMessageRequest->queueName = $queueName;
  $receiveMessageResponse = null;
  while (empty($receiveMessageResponse)) {
    $receiveMessageResponse = $messageClient->receiveMessage($receiveMessageRequest);
  }
  $deleteMessageBatchRequest = new DeleteMessageBatchRequest();
  $deleteMessageBatchRequest->queueName = $queueName;
  foreach ($receiveMessageResponse as $message) {
    echo "Received from default:\n MessageBody: $message->messageBody " .
        "MessageId: $message->messageID ReceiptHandle: $message->receiptHandle\n\n";
    $deleteMessageBatchRequest->deleteMessageBatchRequestEntryList[] =
        new DeleteMessageBatchRequestEntry(
            array('receiptHandle' => $message->receiptHandle));
  }
  $messageClient->deleteMessageBatch($deleteMessageBatchRequest);
  echo "Delete Messages.\n\n";

  $receiveMessageRequest = new ReceiveMessageRequest();
  $receiveMessageRequest->queueName = $queueName;
  $receiveMessageRequest->tagName = $tagName;
  $receiveMessageResponse = null;
  while (empty($receiveMessageResponse)) {
    $receiveMessageResponse = $messageClient->receiveMessage($receiveMessageRequest);
  }
  foreach ($receiveMessageResponse as $message) {
    echo "Received from tag:\n MessageBody: $message->messageBody " .
        "MessageId: $message->messageID ReceiptHandle: $message->receiptHandle\n\n";
  }

  $changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
  $changeMessageVisibilityRequest->queueName = $queueName;
  $changeMessageVisibilityRequest->receiptHandle =
      $receiveMessageResponse[0]->receiptHandle;
  $changeMessageVisibilityRequest->invisibilitySeconds = 0;
  $messageClient->changeMessageVisibilitySeconds($changeMessageVisibilityRequest);
  echo "Change Visibility:\n " .
      "ReceiptHandle: $changeMessageVisibilityRequest->receiptHandle " .
      "Time: $changeMessageVisibilityRequest->invisibilitySeconds Seconds\n\n";

  $receiveMessageRequest->maxReceiveMessageWaitSeconds = 5;
  $receiveMessageResponse = $messageClient->receiveMessage($receiveMessageRequest);
  foreach ($receiveMessageResponse as $message) {
    echo "Receive:\n MessageBody: $message->messageBody " .
        "MessageId: $message->messageID ReceiptHandle: $message->receiptHandle\n\n";
  }

  if (!empty($receiveMessageResponse)) {
    $deleteMessageBatchRequest = new DeleteMessageBatchRequest();
    $deleteMessageBatchRequest->queueName = $queueName;
    foreach ($receiveMessageResponse as $message) {
      $deleteMessageBatchRequest->deleteMessageBatchRequestEntryList[] =
          new DeleteMessageBatchRequestEntry(
              array('receiptHandle' => $message->receiptHandle));
    }
    $messageClient->deleteMessageBatch($deleteMessageBatchRequest);
    echo "Delete Messages.\n\n";
  }

  $setQueueQuotaRequest = new SetQueueQuotaRequest(array(
      'queueName' => $queueName,
      'queueQuota' => new QueueQuota(array(
          'throughput' => new Throughput(array('readQps' => 111, 'writeQps' => 111))
      ))));
  $response = $queueClient->setQueueQuota($setQueueQuotaRequest);
  print_r($response);

  $deleteTagRequest = new DeleteTagRequest();
  $deleteTagRequest->queueName = $queueName;
  $deleteTagRequest->tagName = $tagName;
  $queueClient->deleteTag($deleteTagRequest);

  $deleteQueueRequest = new DeleteQueueRequest();
  $deleteQueueRequest->queueName = $queueName;
  $queueClient->deleteQueue($deleteQueueRequest);
} catch (GalaxyEmqServiceException $e) {
  echo "Failed: $e->errMsg: $e->details : $e->requestId";
} catch (TException $e) {
  echo "Failed: " . $e->getMessage() . "\n\n";
}