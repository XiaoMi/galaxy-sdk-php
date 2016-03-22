<?php
/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */

namespace SDS\Client;


class SdsRequestUtils
{
  public static function getQuery($name, $arguments)
  {
    switch ($name) {
      case 'get':
      case 'put':
      case 'increment':
      case 'remove':
      case 'scan':
      case 'putToRebuildIndex':
        return 'type=' . $name . '&name=' . $arguments[0]->tableName;
      case 'batch':
        $batchItems = $arguments[0]->items;
        switch ($batchItems[0]->action) {
          case BatchOp::PUT:
            $tableName = $batchItems[0]->request->putRequest->tableName;
            break;
          case BatchOp::GET:
            $tableName = $batchItems[0]->request->getRequest->tableName;
            break;
          case BatchOp::INCREMENT:
            $tableName = $batchItems[0]->request->incrementRequest->tableName;
            break;
          case BatchOp::REMOVE:
            $tableName = $batchItems[0]->request->removeRequest->tableName;
            break;
          default:
            throw new \Exception('Unknown batch action' . $batchItems[0]->action);
        }
        return 'type=' . $name . '&name=' . $tableName;
      default:
        return 'type=' . $name;
    }
  }
}