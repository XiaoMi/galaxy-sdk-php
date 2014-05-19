<?php
/**
 * User: heliangliang
 * Date: 5/13/14
 * Time: 10:11 PM
 */

namespace SDS\Client;

use SDS\Table\DataType;
use SDS\Table\Datum;
use SDS\Table\Value;

class DatumUtil
{
  /**
   * @param $value
   * @param $type
   * @return Datum
   * @throws \Exception in case of unsupported data type
   */
  public static function datum($value, $type = null)
  {
    $val = null;
    if (is_null($type)) {
      switch (gettype($value)) {
        case "boolean":
          $type = DataType::BOOL;
          break;
        case "integer":
          $type = DataType::INT64;
          break;
        case "string":
          $type = DataType::STRING;
          break;
        case "double":
          $type = DataType::DOUBLE;
          break;
        default:
          throw new \Exception("Unsupported data type: " . gettype($value));
      }
    }
    switch ($type) {
      case DataType::BOOL:
        $val = new Value(array("boolValue" => $value));
        break;
      case DataType::INT8:
        $val = new Value(array("int8Value" => $value));
        break;
      case DataType::INT16:
        $val = new Value(array("int16Value" => $value));
        break;
      case DataType::INT32:
        $val = new Value(array("int32Value" => $value));
        break;
      case DataType::INT64:
        $val = new Value(array("int64Value" => $value));
        break;
      case DataType::DOUBLE:
        $val = new Value(array("doubleValue" => $value));
        break;
      case DataType::STRING:
        $val = new Value(array("stringValue" => $value));
        break;
      case DataType::BINARY:
        $val = new Value(array("binaryValue" => base64_encode($value)));
        break;
      case DataType::BOOL_SET:
        $val = new Value(array("boolSetValue" => $value));
        break;
      case DataType::INT8_SET:
        $val = new Value(array("int8SetValue" => $value));
        break;
      case DataType::INT16_SET:
        $val = new Value(array("int16SetValue" => $value));
        break;
      case DataType::INT32_SET:
        $val = new Value(array("int32SetValue" => $value));
        break;
      case DataType::INT64_SET:
        $val = new Value(array("int64SetValue" => $value));
        break;
      case DataType::DOUBLE_SET:
        $val = new Value(array("doubleSetValue" => $value));
        break;
      case DataType::STRING_SET:
        $val = new Value(array("stringSetValue" => $value));
        break;
      default:
        throw new \Exception("Unsupported data type: " . DataType::$__names[$type]);
    }
    return new Datum(array("type" => $type, "value" => $val));
  }


  /**
   * @param $datum
   * @return mixed
   * @throws \Exception in case of unsupported data type
   */
  public static function value($datum)
  {
    if (is_null($datum)) {
      return null;
    }

    switch ($datum->type) {
      case DataType::BOOL:
        return $datum->value->boolValue;
      case DataType::INT64:
        return $datum->value->int64Value;
      case DataType::STRING:
        return $datum->value->stringValue;
      case DataType::BINARY:
        return base64_decode($datum->value->binaryValue);
      case DataType::DOUBLE:
        return $datum->value->doubleValue;
      // the following data type is one way conversion since PHP does not support them
      case DataType::INT8:
        return $datum->value->int8Value;
      case DataType::INT16:
        return $datum->value->int16Value;
      case DataType::INT32:
        return $datum->value->int32Value;
      case DataType::FLOAT:
        return $datum->value->doubleValue;
      case DataType::BOOL_SET:
        return $datum->value->boolSetValue;
      case DataType::INT8_SET:
        return $datum->value->int8SetValue;
      case DataType::INT16_SET:
        return $datum->value->int16SetValue;
      case DataType::INT32_SET:
        return $datum->value->int32SetValue;
      case DataType::INT64_SET:
        return $datum->value->int64SetValue;
      case DataType::DOUBLE_SET:
        return $datum->value->doubleSetValue;
      case DataType::STRING_SET:
        return $datum->value->stringSetValue;
      default:
        throw new \Exception("Unsupported data type: " . gettype($datum->type));
    }
  }

  /**
   * @param $data array of datum
   * @return array array of values
   */
  public static function values($data) {
    $values = array();
    foreach ($data as $field => $datum) {
      $values[$field] = DatumUtil::value($datum);
    }
    return $values;
  }
}