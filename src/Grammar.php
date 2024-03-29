<?php
namespace Jfxy\Elasticsearch;

class Grammar
{
    static private $instance;

    protected $commonComponents = [
        'index' => 'index',
//        'type' => 'type',
        'scroll' => 'scroll'
    ];

    protected $scrollComponents = [
        'scroll' => 'scroll',
        'scroll_id' => 'scrollId'
    ];

    protected $selectComponents = [
        '_source' => 'fields',
        'collapse' => 'collapse',
        'min_score' => 'minScore',
        'from' => 'from',
        'size' => 'size',
        'sort' => 'orders',
        'query' => 'wheres',
        'post_filter' => 'postWheres',
        'aggs' => 'aggs',
        'highlight' => 'highlight',
    ];

    protected $operatorMappings = [
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    private function __construct()
    {
    }

    private function __clone()
    {
    }

    static public function getInstance()
    {
        if (!(self::$instance instanceof self)) {
            self::$instance = new self;
        }
        return self::$instance;
    }

    public function compileCommonComponents($builder) :array
    {
        $params = [];
        foreach($this->commonComponents as $k => $v){
            if(!is_null($builder->$v)){
                $method = 'compile' . ucfirst($v);
                $params[$k] = $this->$method($builder);
            }
        }
        return $params;
    }

    public function compileIndex($builder)
    {
        return $builder->index;
    }

    public function compileType($builder)
    {
        return $builder->type;
    }

    public function compileScroll($builder)
    {
        return $builder->scroll;
    }

    public function compileScrollId($builder)
    {
        return $builder->scrollId;
    }

    public function compileIndexParams($builder, array $data,$id = null) :array
    {
        return array_merge($this->compileCommonComponents($builder),[
            'id' => $id,
            'body' => $data
        ]);
    }

    public function compileCreateParams($builder, array $data,string $id) :array
    {
        return array_merge($this->compileCommonComponents($builder),[
            'id' => $id,
            'body' => $data,
        ]);
    }

    public function compileUpdateParams($builder, array $data,string $id) :array
    {
        return array_merge($this->compileCommonComponents($builder),[
            'id' => $id,
            'body' => ['doc' => $data],
        ]);
    }

    public function compileDeleteParams($builder,string $id) :array
    {
        return array_merge($this->compileCommonComponents($builder),[
            'id' => $id
        ]);
    }

    public function compileSearchParams($builder) :array
    {
        return array_merge($this->compileCommonComponents($builder),[
            'body' => $this->compileComponents($builder)
        ]);
    }

    public function compileScrollParams($builder)
    {
        $params = [];
        foreach($this->scrollComponents as $k => $v){
            if(!is_null($builder->$v)){
                $method = 'compile' . ucfirst($v);
                $params[$k] = $this->$method($builder);
            }
        }
        return $params;
    }

    public function compileComponents($builder)
    {
        $dsl = [];
        foreach($this->selectComponents as $k => $v){
            if(!is_null($builder->$v)){
                $method = 'compile' . ucfirst($v);
                $dsl[$k] = $this->$method($builder);
            }
        }
        return empty($dsl) ? new \stdClass() : $dsl;
    }

    public function compileFields($builder) :array
    {
        return $builder->fields;
    }

    public function compileWheres($builder,$filter = false,$not = false) :array
    {
        $whereGroups = $this->wherePriorityGroup($builder->wheres);
        $operation = count($whereGroups) === 1 ? 'must' : 'should';
        $bool = [];
        foreach($whereGroups as $wheres){
            $boolMust = $boolMustNot = $boolFilter = [];
            foreach($wheres as $where){
                if($where['type'] === 'nestedQuery'){
                    $tmp = $this->compileWheres($where['query'],$where['filter'],$where['not']);
                }else{
                    $tmp = $this->whereMatch($where);
                }
                if($where['filter']){
                    $boolFilter[] = $tmp;
                }elseif($where['not']){
                    $boolMustNot[] = $tmp;
                }else{
                    $boolMust[] = $tmp;
                }
            }
            if($operation == 'should'){
                $bool['bool'][$operation] = $bool['bool'][$operation] ?? [];
                $tmp = [];
                if(!empty($boolMust)){
                    if(count($boolMust) === 1 && empty($boolMustNot) && empty($boolFilter)){
                        $tmp = $boolMust[0];
                    }else{
                        $tmp['bool']['must'] = $boolMust;
                    }
                }
                if(!empty($boolMustNot)) {
                    $tmp['bool']['must_not'] = $boolMustNot;
                }
                if(!empty($boolFilter)) {
                    $tmp['bool']['filter'] = $boolFilter;
                }
                array_push($bool['bool'][$operation],$tmp);
            }else{
                if(!empty($boolMust)){
                    if(count($boolMust) === 1 && empty($boolMustNot) && empty($boolFilter)){
                        $bool = $boolMust[0];
                    }else{
                        $bool['bool']['must'] = $boolMust;
                    }
                }
                if(!empty($boolMustNot)){
                    $bool['bool']['must_not'] = $boolMustNot;
                }
                if(!empty($boolFilter)){
                    $bool['bool']['filter'] = $boolFilter;
                }
            }
        }
        if(!is_null($builder->minimumShouldMatch)){
            $bool['bool']['minimum_should_match'] = $builder->minimumShouldMatch;
        }
        if($filter && $not){
            $bool = [
                'bool' => [
                    'must_not' => $bool
                ]
            ];
        }
        return $bool;
    }

    public function compilePostWheres($builder,$filter = false,$not = false) :array
    {
        $whereGroups = $this->wherePriorityGroup($builder->postWheres);
        $operation = count($whereGroups) === 1 ? 'must' : 'should';
        $bool = [];
        foreach($whereGroups as $wheres){
            $boolMust = $boolMustNot = $boolFilter = [];
            foreach($wheres as $where){
                if($where['type'] === 'nestedQuery'){
                    $tmp = $this->compileWheres($where['query'],$where['filter'],$where['not']);
                }else{
                    $tmp = $this->whereMatch($where);
                }
                if($where['filter']){
                    $boolFilter[] = $tmp;
                }elseif($where['not']){
                    $boolMustNot[] = $tmp;
                }else{
                    $boolMust[] = $tmp;
                }
            }
            if($operation == 'should'){
                $bool['bool'][$operation] = $bool['bool'][$operation] ?? [];
                $tmp = [];
                if(!empty($boolMust)){
                    if(count($boolMust) === 1 && empty($boolMustNot) && empty($boolFilter)){
                        $tmp = $boolMust[0];
                    }else{
                        $tmp['bool']['must'] = $boolMust;
                    }
                }
                if (!empty($boolMustNot)) {
                    $tmp['bool']['must_not'] = $boolMustNot;
                }
                if (!empty($boolFilter)) {
                    $tmp['bool']['filter'] = $boolFilter;
                }
                array_push($bool['bool'][$operation],$tmp);
            }else{
                if(!empty($boolMust)){
                    if(count($boolMust) === 1 && empty($boolMustNot) && empty($boolFilter)){
                        $bool = $boolMust[0];
                    }else{
                        $bool['bool']['must'] = $boolMust;
                    }
                }
                if (!empty($boolMustNot)) {
                    $bool['bool']['must_not'] = $boolMustNot;
                }
                if (!empty($boolFilter)) {
                    $bool['bool']['filter'] = $boolFilter;
                }
            }
        }
        if(!is_null($builder->minimumShouldMatch)){
            $bool['bool']['minimum_should_match'] = $builder->minimumShouldMatch;
        }
        if($filter && $not){
            $bool = [
                'bool' => [
                    'must_not' => $bool
                ]
            ];
        }
        return $bool;
    }

    public function compileAggs($builder) :array
    {
        $aggs = [];
        foreach($builder->aggs as $agg){
            $params = $agg['params'];
            if($agg['params'] instanceof Builder){
                $params = $this->compileWheres($agg['params']);
            }
            $aggs[$agg['alias']] = [$agg['type'] => $params];
            if(!empty($agg['subGroups'])){
                $aggs[$agg['alias']]['aggs'] = [];
                foreach($agg['subGroups'] as $subGroup){
                    $aggs[$agg['alias']]['aggs'] = array_merge($aggs[$agg['alias']]['aggs'],$this->compileAggs($subGroup));
                }
            }
        }
        return $aggs;
    }

    public function compileOrders($builder) :array
    {
        return $builder->orders;
    }

    public function compileSize($builder)
    {
        return $builder->size;
    }

    public function compileFrom($builder)
    {
        return $builder->from;
    }

    public function compileCollapse($builder)
    {
        if(is_string($builder->collapse)){
            $collapse = ['field' => $builder->collapse];
        } else {
            $collapse = $builder->collapse;
        }
        return $collapse;
    }

    public function compileMinScore($builder)
    {
        return $builder->minScore;
    }

    public function compileHighlight($builder)
    {
        $highlight = $builder->highlightConfig;
        foreach($builder->highlight as $field => $params){
            $highlight['fields'][$field] = empty($params) ? new \stdClass() : $params;
        }
        return $highlight;
    }

    public function whereMatch(array $where)
    {
        $term = [];
        switch ($where['type']) {
            case 'basic':
                $term = ['term' => [$where['field'] => $where['value']]];
                break;
            case 'match':
            case 'match_phrase':
            case 'match_phrase_prefix':
                $term = [
                    $where['type'] => [
                        $where['field'] => [
                            'query' => $where['value']
                        ]
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term[$where['type']][$where['field']] = array_merge($term[$where['type']][$where['field']],$where['appendParams']);
                }
                break;
            case 'multi_match':
                $term = [
                    'multi_match' => [
                        'query' => $where['value'],
                        'type' => $where['matchType'],
                        'fields' => $where['field']
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term['multi_match'] = array_merge($term['multi_match'],$where['appendParams']);
                }
                break;
            case 'between':
                $term = [];
                foreach($where['value'] as $k => $v){
                    if(is_numeric($k)){
                        if($k == 0){
                            $term['range'][$where['field']][$this->operatorMappings['>=']] = $v;
                        }else{
                            $term['range'][$where['field']][$this->operatorMappings['<=']] = $v;
                        }
                    }else{
                        $term['range'][$where['field']][$this->operatorMappings[$k]] = $v;
                    }
                }
                break;
            case 'in':
                $term = ['terms' => [$where['field'] => $where['value']]];
                break;
            case 'exists':
                $term = ['exists' => ['field' => $where['field']]];
                break;
            case 'prefix':
            case 'wildcard':
            case 'regexp':
            case 'fuzzy':
                $term = [
                    $where['type'] => [
                        $where['field'] => [
                            'value' => $where['value']
                        ]
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term[$where['type']][$where['field']] = array_merge($term[$where['type']][$where['field']],$where['appendParams']);
                }
                break;
            case 'nested':
                $term = [
                    'nested' => [
                        'path' => $where['path'],
                        'query' => $this->compileWheres($where['query'],$where['filter'],$where['not'])
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term['nested'] = array_merge($term['nested'],$where['appendParams']);
                }
                break;
            case 'raw':
                $term = $where['where'];
                break;
        }
        if(@$where['filter'] && $where['not']){
            $term = ['bool'=>['must_not'=>$term]];
        }
        return $term;
    }

    public function wherePriorityGroup($wheres) :array
    {
        $orIndex = array_keys(array_map(function($where){
            return $where['boolean'];
        },$wheres),'or');
        $initIndex = $lastIndex = 0;
        $groups = [];
        foreach($orIndex as $index){
            $items = array_slice($wheres,$initIndex,$index - $initIndex);
            if($items) $groups[] = $items;
            $initIndex = $lastIndex = $index;
        }
        $groups[] = array_slice($wheres, $lastIndex);
        return $groups;
    }
}
