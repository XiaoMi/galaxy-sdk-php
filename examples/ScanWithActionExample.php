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
use SDS\Table\KeySpec;
use SDS\Table\ProvisionThroughput;
use SDS\Table\PutRequest;
use SDS\Table\RemoveRequest;
use SDS\Table\Request;
use SDS\Table\ScanAction;
use SDS\Table\ScanOp;
use SDS\Table\ScanRequest;
use SDS\Table\TableMetadata;
use SDS\Table\TableQuota;
use SDS\Table\TableSchema;
use SDS\Table\TableSpec;

// Set your AppKey and AppSecret
$appKey = "";
$appSecret = "";

$credential = new Credential(
  array(
    'type' => UserType::APP_SECRET,
    'secretKeyId' => $appKey,
    'secretKey' => $appSecret
  )
);
$clientFactory = new ClientFactory($credential, true, false); // verbose off
$endpoint = "http://cnbj-s0.sds.api.xiaomi.com";
$adminClient = $clientFactory->newAdminClient($endpoint .
  Constant::get('ADMIN_SERVICE_PATH'),
  Constant::get('DEFAULT_ADMIN_CLIENT_TIMEOUT'),
  Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT'));
$tableClient = $clientFactory->newTableClient($endpoint .
  Constant::get('TABLE_SERVICE_PATH'),
  Constant::get('DEFAULT_CLIENT_TIMEOUT'),
  Constant::get('DEFAULT_CLIENT_CONN_TIMEOUT'));

$tableName = "php-test-weather";

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
      'primaryIndex' => array(
        new KeySpec(array('attribute' => 'cityId')),
        new KeySpec(array('attribute' => 'timestamp', 'asc' => false))),
      'attributes' => array(
        'cityId' => DataType::STRING,
        'timestamp' => DataType::INT64,
        'score' => DataType::DOUBLE,
        'pm25' => DataType::INT64
      )
    )),
  'metadata' => new TableMetadata(array(
        'quota' => new TableQuota(array('size' => 100 * 1024 * 1024)), // 100MB
        'throughput' => new ProvisionThroughput(array(
            'readCapacity' => 200,
            'writeCapacity' => 200
          ))
      )
    )
));

$adminClient->createTable($tableName, $tableSpec);

// put data
$put = new PutRequest();
$put->tableName = $tableName;

$cities = array("Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou", "Haikou",
  "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
  "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
  "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai");


$now = new \DateTime();
echo "========== put data ==========\n";
for ($i = 0; $i < count($cities); $i++) {
  $put->record = Array(
    'cityId' => DatumUtil::datum($cities[$i]),
    'timestamp' => DatumUtil::datum($now->getTimestamp()),
    'score' => DatumUtil::datum((double)(rand() % 100)),
    'pm25' => DatumUtil::datum(rand() % 500)
  );

  $tableClient->put($put);
  echo "put record #$i\n";
}
echo "========== records in table ==========\n";
dispalyTable($tableName, $tableClient, count($cities));

$scan = new ScanRequest(array(
  'tableName' => $tableName,
  'startKey' => null, // null or unspecified means begin of the table
  'stopKey' => null, // null or unspecified means end of the table
  'limit' => 1,
));

echo "========== scan with action COUNT ==========\n";
$scan->action = new ScanAction(array('action' => ScanOp::COUNT));
$count = scanWithAction($tableClient, $scan);
echo "There are totally $count cities\n";

$scan->startKey = array('cityId' => DatumUtil::datum('Guangzhou'));
$scan->stopKey = array('cityId' => DatumUtil::datum('Qingdao'));
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities between Guangzhou and Qingdao\n";

echo "========== scan with action UPDATE ==========\n";
// set all cities' score 60.0
$scan->startKey = null;
$scan->stopKey = null;
$scan->action = new ScanAction(array(
  'action' => ScanOp::UPDATE, 'request' => new Request(array(
      'putRequest' => new PutRequest(array(
          'record' => array(
            'score' => DatumUtil::datum(60.0)
          )))))));
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities' score are set to 60.0, after update\n";
dispalyTable($tableName, $tableClient, count($cities));

// set the score of cities between Dandong and Sanya (include) to 90.0
$scan->startKey = array('cityId' => DatumUtil::datum('Dandong'));
$scan->stopKey = array('cityId' => DatumUtil::datum('Sanya'));
$scan->action = new ScanAction(array(
  'action' => ScanOp::UPDATE, 'request' => new Request(array(
      'putRequest' => new PutRequest(array(
          'record' => array(
            'score' => DatumUtil::datum(90.0)
          )))))));
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities between Dandong and Sanya and their score are set to 90.0, after update\n";
dispalyTable($tableName, $tableClient, count($cities));

echo "========== scan with action DELETE ==========\n";
// delete pm25 of all cities
$scan->action = new ScanAction(array(
  'action' => ScanOp::DELETE, 'request' => new Request(array(
      'removeRequest' => new RemoveRequest(array(
          'attributes' => array('pm25')
        ))))));
$scan->startKey = null;
$scan->stopKey = null;
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities' pm25 are deleted, after delete\n";
dispalyTable($tableName, $tableClient, count($cities));

// delete score between Ningbo and Tianjin
$scan->action = new ScanAction(array(
  'action' => ScanOp::DELETE, 'request' => new Request(array(
      'removeRequest' => new RemoveRequest(array(
          'attributes' => array('score')
        ))))));

$scan->startKey = array('cityId' => DatumUtil::datum('Ningbo'));
$scan->stopKey = array('cityId' => DatumUtil::datum('Tianjin'));
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities between Ningbo and Tianjin and their score are deleted, after delete\n";
dispalyTable($tableName, $tableClient, count($cities));

// delete cities between Rizhao and Xiamen
$scan->startKey = array('cityId' => DatumUtil::datum('Rizhao'));
$scan->stopKey = array('cityId' => DatumUtil::datum('Xiamen'));
$scan->action = new ScanAction(array(
  'action' => ScanOp::DELETE));
$count = scanWithAction($tableClient, $scan);
echo "There are $count cities between Rizhao and Xiamen and they are deleted, after delete\n";
dispalyTable($tableName, $tableClient, count($cities));

$adminClient->disableTable($tableName);
$adminClient->enableTable($tableName);
$adminClient->dropTable($tableName);

function dispalyTable($tableName, $tableClient, $limit) {
  $scan = new ScanRequest(array(
    'tableName' => $tableName,
    'startKey' => null,
    'stopKey' => null,
    'limit' => $limit,
  ));
  $scanner = new TableScanner($tableClient, $scan);
  foreach ($scanner->iterator() as $v) {
    foreach ($v as $key => $val) {
      $val = DatumUtil::value($val);
      echo "$key: $val\t";
    }
    echo "\n";
  }
}

function scanWithAction($tableClient, $scan) {
  $count = 0;
  $scanner = new TableScanner($tableClient, $scan);
  foreach ($scanner->iterator() as $v) {
    $count += DatumUtil::value($v[Constant::get('SCAN_COUNT')]);
  }
  return $count;
}
