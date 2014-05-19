<?php

require_once dirname(__DIR__) . '/vendor/autoload.php';
require_once __DIR__ . '/SDS/Common/Types.php';
require_once __DIR__ . '/SDS/Auth/Types.php';
require_once __DIR__ . '/SDS/Errors/Types.php';

use Symfony\Component\ClassLoader\UniversalClassLoader;

$loader = new UniversalClassLoader();
$loader->registerNamespace('SDS', __DIR__);
$loader->register();

