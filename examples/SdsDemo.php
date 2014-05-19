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
use SDS\Table\OperatorType;
use SDS\Table\ProvisionThroughput;
use SDS\Table\PutRequest;
use SDS\Table\ScanRequest;
use SDS\Table\SecondaryIndexConsistencyMode;
use SDS\Table\SimpleCondition;
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

$tableName = "php-note";

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
          'enableHash' => true, // hash distribution
        )),
      // Creation time order
      'primaryIndex' => array(
        new KeySpec(array('attribute' => 'noteId', 'asc' => false)),
      ),
      'secondaryIndexes' => array(
        // Default display order, sorted by last modify time
        'mtime' => new LocalSecondaryIndexSpec(array(
            'indexSchema' => array(
              new KeySpec(array('attribute' => 'mtime', 'asc' => false)),
            ),
            'projections' => array('title', 'noteId'),
            'consistencyMode' => SecondaryIndexConsistencyMode::EAGER,
          )),
        // Search by category
        'cat' => new LocalSecondaryIndexSpec(array(
            'indexSchema' => array(
              new KeySpec(array('attribute' => 'category')),
            ),
            'consistencyMode' => SecondaryIndexConsistencyMode::LAZY,
          )),
      ),
      'attributes' => array(
        'userId' => DataType::STRING,
        'noteId' => DataType::INT64,
        'title' => DataType::STRING,
        'content' => DataType::STRING,
        'version' => DataType::INT64,
        'mtime' => DataType::INT64,
        'category' => DataType::STRING_SET,
      )
    )),
  'metadata' => new TableMetadata(array(
      'quota' => new TableQuota(array('size' => 100 * 1024 * 1024)),
      'throughput' => new ProvisionThroughput(array(
          'readQps' => 100,
          'writeQps' => 200
        ))
    ))
));

$adminClient->createTable($tableName, $tableSpec);

// put data
echo "================= insert and update notes ====================\n";
$categories = array("work", "travel", "food");
for ($i = 0; $i < 20; $i++) {
  $version = 0; // initial version
  $insert = new PutRequest(array(
    "tableName" => $tableName,
    "record" => Array(
      "userId" => DatumUtil::datum("user1"),
      "noteId" => DatumUtil::datum($i),
      "title" => DatumUtil::datum("Title $i"),
      "content" => DatumUtil::datum("note $i"),
      "version" => DatumUtil::datum($version),
      "mtime" => DatumUtil::datum($i * $i % 10),
      "category" => DatumUtil::datum(array($categories[$i % sizeof($categories)], $categories[($i + 1) % sizeof($categories)]), DataType::STRING_SET),
    )));
  // insert
  $tableClient->put($insert);

  $put = $insert;
  $put->record["version"] = DatumUtil::datum($version + 1);
  $put->record["content"] = DatumUtil::datum("new content $i");
  $put->record["mtime"] = DatumUtil::datum($i * $i % 10 + 1);
  $put->condition = new SimpleCondition(array(
    "operator" => OperatorType::EQUAL,
    "field" => "version",
    "value" => DatumUtil::datum($version)
  ));
  // update if note is not concurrently modified
  $result = $tableClient->put($put);
  echo "update note without conflict? " . $result->success . "\n";
}

// random access
echo "================= get note by id ====================\n";
$get = new GetRequest(array(
  "tableName" => $tableName,
  "keys" => array(
    "userId" => DatumUtil::datum("user1"),
    "noteId" => DatumUtil::datum(rand(0, 10)),
  ),
));

$result = $tableClient->get($get);
print_r(DatumUtil::values($result->item));

// get noteId which contain category food
echo "================= get notes which contain category food ====================\n";
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "indexName" => "cat",
  "startKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "category" => DatumUtil::datum("food", DataType::STRING),
  ),
  "stopKey" => array(
    "userId" => DatumUtil::datum("user1"),
    "category" => DatumUtil::datum("food", DataType::STRING),
  ),
  "attributes" => array("noteId", "category"),
));
$scanner = new TableScanner($tableClient, $scan);
foreach ($scanner->iterator() as $k => $v) {
  echo "$k: " . DatumUtil::value($v['noteId']) . " [" .
    implode(", ", DatumUtil::value($v['category'])) . "]\n";
}

echo "================= scan by last modify time ====================\n";
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "indexName" => "mtime",
  "startKey" => array(
    "userId" => DatumUtil::datum("user1"),
  ),
  "stopKey" => array(
    "userId" => DatumUtil::datum("user1"),
  ),
  "condition" => "title REGEXP '.*[0-5]' AND noteId > 5",
  "attributes" => array("noteId", "title", "mtime"),
  "limit" => 50 // max records returned for each call, when used with TableScanner
  // this will serve as batch size
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  echo "$k: " . DatumUtil::value($v['noteId'])
    . " [" . DatumUtil::value($v['title']) . "] "
    . DatumUtil::value($v['mtime']) . "\n";
}

$adminClient->dropTable($tableName);