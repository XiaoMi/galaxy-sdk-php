<?php

namespace SDS\Example;

require_once dirname(__DIR__) . "/lib/autoload.php";

use SDS\Auth\Credential;
use SDS\Auth\UserType;
use SDS\Client\ClientFactory;
use SDS\Client\DatumUtil;
use SDS\Client\SdsException;
use SDS\Client\TableScanner;
use SDS\Common\Constant;
use SDS\Errors\ErrorCode;
use SDS\Table\DataType;
use SDS\Table\EntityGroupSpec;
use SDS\Table\GetRequest;
use SDS\Table\KeySpec;
use SDS\Table\LocalSecondaryIndexSpec;
use SDS\Table\ProvisionThroughput;
use SDS\Table\PutRequest;
use SDS\Table\RemoveRequest;
use SDS\Table\ScanRequest;
use SDS\Table\SecondaryIndexConsistencyMode;
use SDS\Table\TableMetadata;
use SDS\Table\TableQuota;
use SDS\Table\TableSchema;
use SDS\Table\TableSpec;

/*************************************************************
 * 通话记录同步应用示例：
 *
 * 对应:MySQL schema:
 * create table calllog (
 * `user_id` bigint '用户ID',
 * `mobile` bigint '用户手机号',
 * `contact` bigint '对方手机号',
 * `time` bigint '通话记录时间',
 * `type` tinyint '类型: 接入，拨出，未接, 拒接',
 * `seq_id` bigint '序列号',
 * `status` tinyint '状态',
 * primary key(`user_id`,`contact`,`time`,`type`,`mobile`),
 * index `sync` (`user_id`,`mobile`,`seq_id`,`status`)
 * )
 **************************************************************/

// Set your appID and AppSecret
$appId = "";
$appSecret = "";

$credential = new Credential(
  array(
    "type" => UserType::APP_SECRET,
    "secretKeyId" => $appId,
    "secretKey" => $appSecret
  )
);
$clientFactory = new ClientFactory($credential, true, false); // verbose off
$endpoint = "https://sds.api.xiaomi.com";
$adminClient = $clientFactory->newAdminClient($endpoint .
  Constant::get('ADMIN_SERVICE_PATH'),
  Constant::get('DEFAULT_ADMIN_CLIENT_TIMEOUT'),
  Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT'));
$tableClient = $clientFactory->newTableClient($endpoint .
  Constant::get('TABLE_SERVICE_PATH'),
  Constant::get('DEFAULT_CLIENT_TIMEOUT'),
  Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT'));

$tableName = "php-calllog";

// drop table
try {
  $adminClient->dropTable($tableName);
} catch (SdsException $e) {
  if ($e->errorCode != ErrorCode::RESOURCE_NOT_FOUND) {
    throw $e;
  }
  echo "dropTable failed: {$e->errorCode}\n";
}

// create table
$tableSpec = new TableSpec(array(
  'schema' => new TableSchema(array(
      'entityGroup' => new EntityGroupSpec(array(
          'attributes' => array(new KeySpec(array('attribute' => 'userId'))),
          'enableHash' => true, // enable hash salt
        )),
      'primaryIndex' => array(
        new KeySpec(array('attribute' => 'contact')),
        new KeySpec(array('attribute' => 'time')),
        new KeySpec(array('attribute' => 'type')),
        new KeySpec(array('attribute' => 'bindId'))),
      'secondaryIndexes' => array(
        // index used for syncing call log
        'sync' => new LocalSecondaryIndexSpec(array(
            'indexSchema' => array(
              new KeySpec(array('attribute' => 'bindId')),
              new KeySpec(array('attribute' => 'seqId')),
              new KeySpec(array('attribute' => 'status'))),
            'consistencyMode' => SecondaryIndexConsistencyMode::LAZY,
          )),
        // eager index test, just ignore
        'time' => new LocalSecondaryIndexSpec(array(
            'indexSchema' => array(
              new KeySpec(array('attribute' => 'time')),
              new KeySpec(array('attribute' => 'contact'))),
            'projections' => array('seqId', 'status', 'test'),
            'consistencyMode' => SecondaryIndexConsistencyMode::EAGER,
          )),
      ),
      'attributes' => array(
        'userId' => DataType::STRING,
        'contact' => DataType::STRING,
        'time' => DataType::INT64,
        'type' => DataType::INT8,
        'bindId' => DataType::STRING,
        'seqId' => DataType::INT64,
        'status' => DataType::INT8,
        'test' => DataType::STRING,
        'foobar' => DataType::INT8,
      ),
    )),
  'metadata' => new TableMetadata(array(
      'quota' => new TableQuota(array('size' => 100 * 1024 * 1024)), // 100MB
      'throughput' => new ProvisionThroughput(array(
          'readQps' => 20,
          'writeQps' => 100
        ))
    ))
));

$adminClient->createTable($tableName, $tableSpec);
$adminClient->describeTable($tableName);
$adminClient->findAllTables();
$tableSpec->schema->attributes = array(
  'userId' => DataType::STRING,
  'contact' => DataType::STRING,
  'time' => DataType::INT64,
  'type' => DataType::INT8,
  'bindId' => DataType::STRING,
  'seqId' => DataType::INT64,
  'status' => DataType::INT8,
  'test' => DataType::STRING,
  'foobar2' => DataType::STRING,
);
$adminClient->disableTable($tableName);
$adminClient->alterTable($tableName, $tableSpec);
$adminClient->enableTable($tableName);
$adminClient->describeTable($tableName);
$adminClient->findAllTables();

// put data
$put = new PutRequest();
$put->tableName = $tableName;

$bindId = "13800000000";
$now = new \DateTime();
$time = $now->getTimestamp();

echo "================= put ====================\n";
for ($i = 0; $i < 20; $i++) {
  $time += $i % 2; // test non-unique index
  $put->record = Array(
    "userId" => DatumUtil::datum("user1"),
    "contact" => DatumUtil::datum("10086" . ($i % 3)),
    "time" => DatumUtil::datum($time),
    "type" => DatumUtil::datum($i % 4, DataType::INT8),
    "bindId" => DatumUtil::datum($bindId),
    "seqId" => DatumUtil::datum($i),
    "status" => DatumUtil::datum(0, DataType::INT8),
    "test" => DatumUtil::datum("call log #${i}"),
  );

  $tableClient->put($put);
  if ($i % 2 == 1) { // test delete, just ignore
    $remove = new RemoveRequest();
    $remove->tableName = $tableName;
    $remove->keys = $put->record;
    $tableClient->remove($remove);
  }
  echo "put record #$i\n";
}

// random get
echo "================= random get ====================\n";
$get = new GetRequest(array(
  "tableName" => $tableName,
  "keys" => array(
    "userId" => DatumUtil::datum("user1"),
    "contact" => DatumUtil::datum("100860"),
    "time" => DatumUtil::datum($now->getTimestamp()),
    "type" => DatumUtil::datum(0, DataType::INT8),
    "bindId" => DatumUtil::datum($bindId),
  ),
));

$result = $tableClient->get($get);
print_r(DatumUtil::values($result->item));

// scan primary index
echo "================= primary index scan ====================\n";
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "startKey" => array(
    "userId" => DatumUtil::datum("user1"),
  ),
  "stopKey" => array(
    "userId" => DatumUtil::datum("user1"),
  ),
  // Take care when use filter condition, DO NOT use if it filters out the majority of
  // scanned records in the range, it can be hidden performance bottleneck
  "condition" => "type != 0 AND NOT contact REGEXP '.+0'", // SQL WHERE like expression
  "limit" => 10 // max records returned for each call, when used with TableScanner
                // this will serve as batch size
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  $seqId = DatumUtil::value($v['seqId']);
  $bindId = DatumUtil::value($v['bindId']);
  $contact = DatumUtil::value($v['contact']);
  $type = DatumUtil::value($v['type']);
  $time = DatumUtil::value($v['time']);
  echo "$k: $contact $time $type $bindId $seqId\n";
}

// sync call log, scan with sync index
echo "================= sync call log to max seqId ====================\n";
$startTag = 5; // client seqId
$endTag = 20; // max seqId, can be defined in another table with an auto-increment column
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "indexName" => "sync",
  "startKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "bindId" => DatumUtil::datum($bindId),
    "seqId" => DatumUtil::datum($startTag),
  ),
  "stopKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "bindId" => DatumUtil::datum($bindId),
    "seqId" => DatumUtil::datum($endTag),
  ),
  "limit" => 2
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  $seqId = DatumUtil::value($v['seqId']);
  $status = DatumUtil::value($v['status']);
  $bindId = DatumUtil::value($v['bindId']);
  $contact = DatumUtil::value($v['contact']);
  $type = DatumUtil::value($v['type']);
  $time = DatumUtil::value($v['time']);
  echo "$k: $bindId $seqId $contact $type $status $time\n";
}

// test eager index, range scan, and reverse scan
echo "================= query by time, in reverse order ====================\n";
$startTime = $now->getTimestamp();
$endTime = $startTime + 5;
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "indexName" => "time",
  "startKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "time" => DatumUtil::datum($endTime), // reverse
  ),
  "stopKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "time" => DatumUtil::datum($startTime),
  ),
  "reverse" => true, // Note: avoiding use reverse scan if possible, it's slower than normal scan
  "limit" => 2
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  $seqId = DatumUtil::value($v['seqId']);
  $bindId = DatumUtil::value($v['bindId']);
  $contact = DatumUtil::value($v['contact']);
  $type = DatumUtil::value($v['type']);
  $time = DatumUtil::value($v['time']);
  echo "$k: $time $contact $seqId $bindId $type\n";
}

$adminClient->disableTable($tableName);
$adminClient->enableTable($tableName);
$adminClient->dropTable($tableName);
