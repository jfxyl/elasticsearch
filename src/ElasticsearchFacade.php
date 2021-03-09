<?php

namespace Jfxy\Elasticsearch;

use Illuminate\Support\Facades\Facade;

class ElasticsearchFacade extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'es';
    }
}
