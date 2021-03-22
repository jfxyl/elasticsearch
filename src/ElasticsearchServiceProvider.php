<?php

namespace Jfxy\Elasticsearch;

use Illuminate\Support\ServiceProvider;
use Illuminate\Contracts\Support\DeferrableProvider;

class ElasticsearchServiceProvider extends ServiceProvider implements DeferrableProvider
{

    protected $defer = true;

    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot()
    {
        $this->publishes([
            __DIR__.'/../config/es.php' => config_path('es.php'),
        ]);
    }

    /**
     * Register any application services.
     *
     * @return void
     */
    public function register()
    {
        $this->app->singleton('es', function ($app) {
            return new Builder();
        });
    }

    public function provides()

    {
        // 因为延迟加载 所以要定义 provides 函数 具体参考laravel 文档

        return [Builder::class];

    }
}
