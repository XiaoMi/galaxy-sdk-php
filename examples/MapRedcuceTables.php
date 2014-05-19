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
use SDS\Table\KeySpec;
use SDS\Table\ProvisionThroughput;
use SDS\Table\PutRequest;
use SDS\Table\ScanRequest;
use SDS\Table\TableMetadata;
use SDS\Table\TableQuota;
use SDS\Table\TableSchema;
use SDS\Table\TableSpec;

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

$tableIn = "mapreduce-in";
$tableOut = "mapreduce-out";

// drop table
try {
  $adminClient->dropTable($tableIn);
} catch (SdsException $e) {
  if ($e->errorCode != ErrorCode::RESOURCE_NOT_FOUND) {
    throw $e;
  }
  echo "dropTable failed: {$e->errorCode}\n";
}
try {
  $adminClient->dropTable($tableOut);
} catch (SdsException $e) {
  if ($e->errorCode != ErrorCode::RESOURCE_NOT_FOUND) {
    throw $e;
  }
  echo "dropTable failed: {$e->errorCode}\n";
}

// create table
$tableSpecIn = new TableSpec(array(
  'schema' => new TableSchema(array(
      'preSplits' => 4,
      'entityGroup' => new EntityGroupSpec(array(
          'attributes' => array(new KeySpec(array('attribute' => 'userId'))),
          'enableHash' => true, // enable hash salt
        )),
      'primaryIndex' => array(new KeySpec(array('attribute' => 'seqId'))),
      'attributes' => array(
        'userId' => DataType::STRING,
        'seqId' => DataType::INT64,
        'content' => DataType::STRING,
      ),
    )),
  'metadata' => new TableMetadata(array(
      'quota' => new TableQuota(array('size' => 100 * 1024 * 1024)), // 100MB
      'throughput' => new ProvisionThroughput(array(
          'readQps' => 1000,
          'writeQps' => 1000
        ))
    ))
));

$tableSpecOut = new TableSpec(array(
  'schema' => new TableSchema(array(
      'primaryIndex' => array(new KeySpec(array('attribute' => 'userId'))),
      'attributes' => array(
        'userId' => DataType::STRING,
        'count' => DataType::INT64
      )
    )),
  'metadata' => new TableMetadata(array(
        'quota' => new TableQuota(array("size" => 100 * 1024 * 1024)), // 100MB
        'throughput' => new ProvisionThroughput(array(
            "readQps" => 1000,
            "writeQps" => 1000
          ))
      )
    )
));

$adminClient->createTable($tableIn, $tableSpecIn);
$adminClient->createTable($tableOut, $tableSpecOut);

// put data
$put = new PutRequest();
$put->tableName = $tableIn;

for ($i = 0; $i < 10; $i++) {
  for ($j = 0; $j < 10; $j++) {
    $put->record = Array(
      "userId" => DatumUtil::datum("user" . $i),
      "seqId" => DatumUtil::datum($j),
      "content" => DatumUtil::datum("content-" . $i . "-" . $j),
    );

    $tableClient->put($put);
    echo "put record #$i-$j\n";
  }
}


// scan table
$scan = new ScanRequest(array(
  "tableName" => $tableIn,
  "startKey" => null, // null or unspecified means begin of the table
  "stopKey" => null, // null or unspecified means end of the table
  "limit" => 100 // max records returned for each call, when used with TableScanner
                 // this will serve as batch size
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  // print_r(array($k => DatumUtil::values($v)));
  $content = DatumUtil::value($v['content']);
  echo "$k $content\n";
}

#$adminClient->dropTable($tableName);
