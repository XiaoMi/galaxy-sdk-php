<?php

namespace SDS\Example;

require_once dirname(__DIR__) . "/lib/autoload.php";

use SDS\Auth\Credential;
use SDS\Auth\UserType;
use SDS\Client\ClientFactory;
use SDS\Client\DatumUtil;
use SDS\Client\ExactLimitScanner;
use SDS\Client\SdsException;
use SDS\Client\TableScanner;
use SDS\Common\Constant;
use SDS\Errors\ErrorCode;
use SDS\Table\DataType;
use SDS\Table\GetRequest;
use SDS\Table\IncrementRequest;
use SDS\Table\KeySpec;
use SDS\Table\ProvisionThroughput;
use SDS\Table\PutRequest;
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
    "type" => UserType::APP_SECRET,
    "secretKeyId" => $appKey,
    "secretKey" => $appSecret
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
        'quota' => new TableQuota(array("size" => 100 * 1024 * 1024)), // 100MB
        'throughput' => new ProvisionThroughput(array(
            "readCapacity" => 20,
            "writeCapacity" => 20
          ))
      )
    )
));

$adminClient->createTable($tableName, $tableSpec);
print_r($adminClient->describeTable($tableName));

// put data
$put = new PutRequest();
$put->tableName = $tableName;


$cities = array("北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou", "Haikou",
  "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
  "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
  "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai");


$now = new \DateTime();

for ($i = 0; $i < 10; $i++) {
  $put->record = Array(
    "cityId" => DatumUtil::datum($cities[$i]),
    "timestamp" => DatumUtil::datum($now->getTimestamp()),
    "score" => DatumUtil::datum((double)(rand() % 100)),
    "pm25" => DatumUtil::datum(rand() % 500)
  );

  $tableClient->put($put);
  echo "put record #$i\n";
}

// get data
$get = new GetRequest(array(
  "tableName" => $tableName,
  "keys" => array(
    "cityId" => DatumUtil::datum($cities[0]),
    "timestamp" => DatumUtil::datum($now->getTimestamp()),
  ),
  "attributes" => array("pm25") // get all attributes if not specified
));

$result = $tableClient->get($get);
echo "before increment:\n";
print_r(DatumUtil::values($result->item));

$inc = new IncrementRequest(array(
  "tableName" => $tableName,
  "keys" => array(
    "cityId" => DatumUtil::datum($cities[0]),
    "timestamp" => DatumUtil::datum($now->getTimestamp()),
  ),
  "amounts" => array("pm25" => DatumUtil::datum(10))
));
$tableClient->increment($inc);
echo "after increase pm25 10, the result:\n";
$result = $tableClient->get($get);
print_r(DatumUtil::values($result->item));


// scan table
$scan = new ScanRequest(array(
  "tableName" => $tableName,
  "startKey" => null, // null or unspecified means begin of the table
  "stopKey" => null, // null or unspecified means end of the table
  "attributes" => array("cityId", "score"), // scan all attributes if not specified
  "condition" => "score > 50", // condition to meet
  "limit" => 2 // max records returned for each call, when used with TableScanner
               // this will serve as batch size
));

$scanner = new TableScanner($tableClient, $scan);

foreach ($scanner->iterator() as $k => $v) {
  // print_r(array($k => DatumUtil::values($v)));
  $city = DatumUtil::value($v['cityId']);
  $score = DatumUtil::value($v['score']);
  echo "$k $city: $score\n";
}

$exactScan = new ScanRequest(array(
  "tableName" => $tableName,
  "startKey" => null, // null or unspecified means begin of the table
  "stopKey" => null, // null or unspecified means end of the table
  "attributes" => array("cityId"), // scan all attributes if not specified
  "limit" => 5 // max records returned for each call, when used with TableScanner
  // this will serve as batch size
));

$result = ExactLimitScanner::scan($tableClient, $exactScan);

foreach ($result as $k => $v) {
  $city = DatumUtil::value($v['cityId']);
  echo "$k $city\n";
}


$adminClient->disableTable($tableName);
$adminClient->enableTable($tableName);
$adminClient->dropTable($tableName);
