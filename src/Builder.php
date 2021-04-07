<?php

namespace Jfxy\Elasticsearch;

use \Closure;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class Builder
{
    protected $client;

    protected $grammar;

    public $index;

    public $type;

    public $wheres;

    public $postWheres;

    public $fields;

    public $from;

    public $size;

    public $orders;

    public $aggs;

    public $collapse;

    public $highlight;

    public $raw;

    public $dsl;

    public $scroll;

    public $scrollId;

    public $minimumShouldMatch;

    public $minScore;

    public $highlightConfig = [];

    protected $response;

    protected $operators = [
        '=', '>', '<', '>=', '<=', '!=', '<>'
    ];

    public function __construct($config = [], $init = true)
    {
        // 当使用闭包嵌套时，会通过newQuery方法实例化当前类时，设置$init = false，避免在每一个闭包中都进行es客户端的实例化
        if ($init) {
            $this->config = $config;
            $this->client = $this->clientBuilder();
        }
        $this->grammar = new Grammar();
    }

    /**
     * @param array $config
     * @param bool $init
     * @return Builder
     */
    public static function init($config = [], $init = true): self
    {
        return new static($config, $init);
    }

    /**
     * @return Client
     */
    protected function clientBuilder()
    {
        $client = ClientBuilder::create();

        if (isset($this->config['hosts'])) $client->setHosts($this->config['hosts']);
        if (isset($this->config['connection_pool'])) $client->setConnectionPool($this->config['connection_pool']);
        if (isset($this->config['selector'])) $client->setSelector($this->config['selector']);
        if (isset($this->config['serializer'])) $client->setSerializer($this->config['serializer']);
        if (isset($this->config['connection_retry_times'])) $client->setRetries($this->config['connection_retry_times']);

        return $client->build();
    }

    /**
     * @param $client
     * @return $this
     */
    public function setClient($client): self
    {
        $this->client = $client;
        return $this;
    }

    /**
     * @return \Elasticsearch\Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param $index
     * @return $this
     */
    public function setIndex($index): self
    {
        $this->index = $index;
        return $this;
    }

    /**
     * @param $type
     * @return $this
     */
    public function setType($type): self
    {
        $this->type = $type;
        return $this;
    }

    /**
     * 索引文档
     * @param array $data
     * @param null $id
     * @return mixed
     */
    public function index(array $data, $id = null)
    {
        return $this->run($this->grammar->compileIndexParams($this, $data, $id), 'index');
    }

    /**
     * 创建文档
     * @param array $data
     * @param null $id
     * @return mixed
     */
    public function create(array $data, string $id)
    {
        return $this->run($this->grammar->compileCreateParams($this, $data, $id), 'create');
    }

    /**
     * 更新文档
     * @param $id
     * @param array $data
     * @return mixed
     */
    public function update(array $data, string $id)
    {
        return $this->run($this->grammar->compileUpdateParams($this, $data, $id), 'update');
    }

    /**
     * 删除文档
     * @param $id
     * @return bool
     */
    public function delete(string $id)
    {
        return $this->run($this->grammar->compileDeleteParams($this, $id), 'delete');
    }

    /**
     * @param $fields
     * @return $this
     */
    public function select($fields): self
    {
        $this->fields = is_array($fields) ? $fields : func_get_args();

        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $match
     * @param string $boolean
     * @param bool $not
     * @return $this
     * @throws Exception
     */
    public function where($field, $operator = null, $value = null, $boolean = 'and', $not = false, $filter = false): self
    {
        if (is_array($field)) {
            return $this->addArrayOfWheres($field, $boolean, $not, $filter);
        }
        if ($field instanceof Closure && is_null($operator)) {
            return $this->nestedQuery($field, $boolean, $not, $filter);
        }

        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        if (in_array($operator, ['!=', '<>'])) {
            $not = !$not;
        }

        if (is_array($value)) {
            return $this->whereIn($field, $value, $boolean, $not, $filter);
        }

        if (in_array($operator, ['>', '<', '>=', '<='])) {
            $value = [$operator => $value];
            return $this->whereBetween($field, $value, $boolean, $not, $filter);
        }

        $type = 'basic';

        $this->wheres[] = compact(
            'type', 'field', 'operator', 'value', 'boolean', 'not', 'filter'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function orWhere($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'or');
    }


    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function whereNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function orWhereNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'or', true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function filter($field, $operator = null, $value = null, $boolean = 'and', $not = false): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, $boolean, $not, true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function orFilter($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'or');
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function filterNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'and', true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function orFilterNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'or', true);
    }

    /**
     * 单字段查询
     * @param $field
     * @param null $value
     * @param string $type match|match_phrase|match_phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMatch($field, $value = null, $type = 'match', array $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $this->wheres[] = compact(
            'type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereMatch($field, $value = null, $type = 'match', array $appendParams = []): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', false);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMatch($field, $value = null, $type = 'match', array $appendParams = []): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMatch($field, $value = null, $type = 'match', array $appendParams = []): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', true);
    }

    /**
     * 多字段查询
     * @param $field
     * @param null $value
     * @param string $type best_fields|most_fields|cross_fields|phrase|phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        [$type, $matchType] = ['multi_match', $type];
        $this->wheres[] = compact(
            'type', 'field', 'value', 'matchType', 'appendParams', 'boolean', 'not', 'filter'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = []): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', false);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = []): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = []): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', true);
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereIn($field, array $value, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'in';
        $this->wheres[] = compact('type', 'field', 'value', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotIn($field, array $value): self
    {
        return $this->whereIn($field, $value, 'and', true);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereIn($field, array $value): self
    {
        return $this->whereIn($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotIn($field, array $value): self
    {
        return $this->whereNotIn($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereBetween($field, array $value, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'between';
        $this->wheres[] = compact('type', 'field', 'value', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotBetween($field, array $value): self
    {
        return $this->whereBetween($field, $value, 'and', true);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereBetween($field, array $value): self
    {
        return $this->whereBetween($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotBetween($field, array $value): self
    {
        return $this->whereBetween($field, $value, 'or', true);
    }

    /**
     * @param $field
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereExists($field, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'exists';
        $this->wheres[] = compact('type', 'field', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @return $this
     */
    public function whereNotExists($field): self
    {
        return $this->whereExists($field, 'and', true);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereExists($field): self
    {
        return $this->whereExists($field, 'or');
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereNotExists($field): self
    {
        return $this->whereExists($field, 'or', true);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function wherePrefix($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'prefix';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotPrefix($field, $value, $appendParams = []): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWherePrefix($field, $value, $appendParams = []): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotPrefix($field, $value, $appendParams = []): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'or', true);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereWildcard($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'wildcard';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotWildcard($field, $value, $appendParams = []): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereWildcard($field, $value, $appendParams = []): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotWildcard($field, $value, $appendParams = []): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'or', true);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereRegexp($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'regexp';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotRegexp($field, $value, $appendParams = []): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereRegexp($field, $value, $appendParams = []): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotRegexp($field, $value, $appendParams = []): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'or', true);
    }


    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereFuzzy($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'fuzzy';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotFuzzy($field, $value, $appendParams = []): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereFuzzy($field, $value, $appendParams = []): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotFuzzy($field, $value, $appendParams = []): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'or', true);
    }

    /**
     * nested类型字段查询
     * @param $path
     * @param $wheres
     * @param $appendParams
     * @return $this
     * @throws Exception
     */
    public function whereNested($path, $wheres, $appendParams = []): self
    {
        if (!($wheres instanceof Closure) && !is_array($wheres)) {
            throw new \InvalidArgumentException('非法参数');
        }
        $type = 'nested';
        $boolean = 'and';
        $not = false;
        $filter = false;
        $query = $this->newQuery()->where($wheres);
        $this->wheres[] = compact('type', 'path', 'query', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $where
     * @param string $boolean
     * @param bool $not
     * @return Builder
     */
    public function whereRaw($where, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'raw';
        $where = is_string($where) ? json_decode($where, true) : $where;
        $this->wheres[] = compact('type', 'where', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $where
     * @return Builder
     */
    public function orWhereRaw($where): self
    {
        return $this->whereRaw($where, 'or');
    }

    /**
     * 后置过滤器
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @param bool $not
     * @return Builder
     * @throws Exception
     */
    public function postWhere($field, $operator = null, $value = null, $boolean = 'and', $not = false, $filter = false): self
    {
        $query = $this->newQuery()->where(...func_get_args());
        $this->postWheres = is_array($this->postWheres) ? $this->postWheres : [];
        array_push($this->postWheres, ...$query->wheres);
        return $this;
    }

    /**
     * @param  mixed $value
     * @param  callable $callback
     * @param  callable|null $default
     * @return mixed|$this
     */
    public function when($value, $callback, $default = null): self
    {
        if ($value) {
            return $callback($this, $value) ?: $this;
        } elseif ($default) {
            return $default($this, $value) ?: $this;
        }
        return $this;
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function collapse(string $field, array $appendParams = []): self
    {
        if (empty($appendParams)) {
            $this->collapse = $field;
        } else {
            $this->collapse = array_merge(['field' => $field], $appendParams);
        }
        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function from(int $value): self
    {
        $this->from = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function size(int $value): self
    {
        $this->size = $value;

        return $this;
    }

    /**
     * @param string $field
     * @param string $sort
     * @return $this
     */
    public function orderBy(string $field, $sort = 'asc'): self
    {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * 高亮字段
     * @param string $field
     * @param array $params
     * [
     *      "number_of_fragments" => 0  // 字段片段数
     *      ...
     * ]
     * @return $this
     */
    public function highlight(string $field, array $params = []): self
    {
        $this->highlight[$field] = $params;
        return $this;
    }

    /**
     * 高亮配置
     * @param array $config
     * [
     *      "require_field_match" => false,     // 是否只高亮查询的字段
     *      "number_of_fragments" => 1,         // 高亮字段会被分段，返回分段的个数，设置0不分段
     *      "pre_tags" => "<em>",
     *      "post_tags" => "</em>",
     *      ...
     * ]
     * @return $this
     */
    public function highlightConfig(array $config = []): self
    {
        $this->highlightConfig = array_merge($this->highlightConfig, $config);
        return $this;
    }

    /**
     * @param $value
     * @return $this
     */
    public function minimumShouldMatch($value): self
    {
        $this->minimumShouldMatch = $value;
        return $this;
    }

    /**
     * @param $value
     * @return Builder
     */
    public function minScore($value): self
    {
        $this->minScore = $value;
        return $this;
    }

    /**
     * @param string $scroll
     * @return $this
     */
    public function scroll($scroll = '2m'): self
    {
        $this->scroll = $scroll;
        return $this;
    }

    /**
     * @param string $scrollId
     * @return $this
     */
    public function scrollId(string $scrollId): self
    {
        if (empty($this->scroll)) {
            $this->scroll();
        }
        $this->scrollId = $scrollId;
        return $this;
    }

    /**
     * @param string $field
     * @param string $type 常用聚合[terms,histogram,date_histogram,date_range,range,cardinality,avg,sum,min,max,extended_stats...]
     * @param array $appendParams 聚合需要携带的参数，聚合不同参数不同，部分聚合必须传入，比如date_histogram需传入[interval=>day,hour...]
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggs(string $alias, string $type = 'terms', $params = [], ... $subGroups): self
    {
        $aggs = [
            'type' => $type,
            'alias' => $alias,
            'params' => $params,
        ];
        foreach ($subGroups as $subGroup) {
            call_user_func($subGroup, $query = $this->newQuery());
            $aggs['subGroups'][] = $query;
        }
        $this->aggs[] = $aggs;
        return $this;
    }

    /**
     * terms 聚合
     * @param string $field
     * @param array $appendParams 聚合需要携带的参数
     *      [
     *          'size' => 10,                   // 默认
     *          'order' => ['_count'=>'desc']   // 默认
     *          'order' => ['_count'=>'asc']
     *          'order' => ['_key'=>'desc']
     *          'order' => ['_key'=>'asc']
     *          ...
     *      ]
     * @param mixed ...$subGroups
     * @return $this
     */
    public function groupBy(string $field, array $appendParams = [], ... $subGroups): self
    {
        $alias = $field . '_terms';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'terms', $params, ... $subGroups);
    }

    /**
     * date_histogram 聚合
     * @param string $field
     * @param string $interval [year,quarter,month,week,day,hour,minute,second,1.5h,1M...]
     * @param string $format 年月日时分秒的表示方式 [yyyy-MM-dd HH:mm:ss]
     * @param array $appendParams
     * @param mixed ...$subGroups
     * @return $this
     */
    public function dateGroupBy(string $field, string $interval = 'day', string $format = "yyyy-MM-dd", array $appendParams = [], ... $subGroups): self
    {
        $alias = $field . '_date_histogram';
        $params = array_merge([
            'field' => $field,
            'interval' => $interval,
            'format' => $format,
            'min_doc_count' => 0,
        ], $appendParams);
        return $this->aggs($alias, 'date_histogram', $params, ... $subGroups);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function cardinality(string $field, array $appendParams = []): self
    {
        $alias = $field . '_cardinality';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'cardinality', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function avg(string $field, array $appendParams = []): self
    {
        $alias = $field . '_avg';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'avg', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function sum(string $field, array $appendParams = []): self
    {
        $alias = $field . '_sum';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'sum', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function min(string $field, array $appendParams = []): self
    {
        $alias = $field . '_min';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'min', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function max(string $field, array $appendParams = []): self
    {
        $alias = $field . '_max';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'max', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function stats(string $field, array $appendParams = []): self
    {
        $alias = $field . '_stats';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'stats', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function extendedStats(string $field, array $appendParams = []): self
    {
        $alias = $field . '_extended_stats';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'extended_stats', $params);
    }

    /**
     * @param $params
     * @return $this
     */
    public function topHits($params): self
    {
        if (!($params instanceof Closure) && !is_array($params)) {
            throw new \InvalidArgumentException('非法参数');
        }
        if ($params instanceof Closure) {
            call_user_func($params, $query = $this->newQuery());
            $params = $query->dsl();
        }
        return $this->aggs('top_hits', 'top_hits', $params);
    }

    /**
     * 聚合内部进行条件过滤
     * @param string $alias 别名
     * @param callable|array $wheres
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggsFilter(string $alias, $wheres, ... $subGroups): self
    {
        return $this->aggs($alias, 'filter', $this->newQuery()->where($wheres), ... $subGroups);
    }

    /**
     * @param $dsl
     * @return $this
     */
    public function raw($dsl)
    {
        $this->raw = $dsl;
        return $this;
    }

    /**
     * 返回dsl语句
     * @param string $type
     * @return array|false|string|null
     */
    public function dsl($type = 'array')
    {
        if (!empty($this->raw)) {
            $this->dsl = $this->raw;
        } else {
            $this->dsl = $this->grammar->compileComponents($this);
        }
        if (!is_string($this->dsl) && $type == 'json') {
            $this->dsl = json_encode($this->dsl, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
        }
        return $this->dsl;
    }

    /**
     * @return mixed
     */
    public function count()
    {
        $result = $this->run($this->grammar->compileSearchParams($this), 'count');
        return $result['count'];
    }

    public function get()
    {
        $result = $this->response();

        $list = array_map(function ($hit) {
            return array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
        }, $result['hits']['hits']);
        $data = [
            'total' => $result['hits']['total']['value'],
            'list' => $list
        ];
        if (isset($result['aggregations'])) {
            $data['aggs'] = $result['aggregations'];
        }
        if (isset($result['_scroll_id'])) {
            $data['scroll_id'] = $result['_scroll_id'];
        }
        return $data;
    }

    public function first()
    {
        $this->size(1);
        $result = $this->response();
        $data = null;
        if (isset($result['hits']['hits'][0])) {
            $hit = $result['hits']['hits'][0];
            $data = array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
        }
        return $data;
    }

    public function paginator(int $page = 1, int $size = 10)
    {
        $from = ($page - 1) * $size;

        $this->from($from);

        $this->size($size);

        if (!empty($this->collapse)) {
            $collapse_field = is_string($this->collapse) ? $this->collapse : $this->collapse['field'];
            $this->cardinality($collapse_field);
        }
        $result = $this->response();
        $original_total = $total = $result['hits']['total']['value'];
        if (!empty($this->collapse)) {
            $total = $result['aggregations'][$collapse_field . '_cardinality']['value'];
        }
        $list = array_map(function ($hit) {
            return array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
        }, $result['hits']['hits']);
        $max_page = intval(ceil($total / $size));
        $data = [
            'total' => $total,
            'original_total' => $original_total,
            'per_page' => $size,
            'current_page' => $page,
            'last_page' => $max_page,
            'list' => $list
        ];
        if (isset($result['aggregations'])) {
            $data['aggs'] = $result['aggregations'];
        }
        return $data;
    }

    /**
     * @return mixed
     */
    public function response()
    {
        if (is_null($this->scrollId)) {
            return $this->run($this->grammar->compileSearchParams($this), 'search');
        } else {
            return $this->run($this->grammar->compileScrollParams($this), 'scroll');
        }
    }

    /**
     * 执行elasticsearch操作
     * @param array $params
     * @param $method
     * @return mixed
     */
    protected function run($params = [], $method)
    {
        if (!$this->client instanceof \Elasticsearch\Client) {
            throw new \RuntimeException('需要先配置elasticsearch client哦');
        }
        $result = call_user_func([$this->client, $method], $params);
        return $result;
    }

    protected function addArrayOfWheres($field, $boolean = 'and', $not = false, $filter = false)
    {
        return $this->nestedQuery(function (self $query) use ($field, $not, $filter) {
            foreach ($field as $key => $value) {
                if (is_numeric($key) && is_array($value)) {
                    $query->where(...$value);
                } else {
                    $query->where($key, '=', $value);
                }
            }
        }, $boolean, $not, $filter);
    }

    protected function nestedQuery(Closure $callback, $boolean = 'and', $not = false, $filter = false): self
    {
        call_user_func($callback, $query = $this->newQuery());
        if (!empty($query->wheres)) {
            $type = 'nestedQuery';
            $this->wheres[] = compact('type', 'query', 'boolean', 'not', 'filter');
        }
        return $this;
    }

    /**
     * 闭包内部使用
     * @return Builder
     */
    protected function newQuery()
    {
        return new static(false);
    }

    protected function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            return [$operator, '='];
        } elseif (is_null($value) && in_array($operator, $this->operators)) {
            throw new \InvalidArgumentException('非法运算符和值组合');
        } elseif (is_array($value) && !in_array($operator, ['=', '!=', '<>'])) {
            throw new \InvalidArgumentException('非法运算符和值组合');
        }
        return [$value, $operator];
    }
}
