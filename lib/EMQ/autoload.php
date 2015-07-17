<?php

require_once dirname(__DIR__) . '/../vendor/autoload.php';

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

use Symfony\Component\ClassLoader\UniversalClassLoader;
$loader = new UniversalClassLoader();
$loader->registerNamespace('EMQ', __DIR__);
$loader->registerNamespace('EMQ', __DIR__);
$loader->register();