<?php
function __autoload($class_name)
{
  $directorys = array(
      dirname(__DIR__) . "/",
      dirname(dirname(__DIR__)) . "/"
  );

  foreach ($directorys as $directory) {
    $class_name = str_replace("\\","/",$class_name);
    if (file_exists($directory . $class_name . '.php')) {
      require_once($directory . $class_name . '.php');
      return;
    }
  }
}