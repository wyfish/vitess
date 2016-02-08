<?php
// DO NOT EDIT! Generated by Protobuf-PHP protoc plugin 1.0
// Source: vtgate.proto
//   Date: 2016-01-22 01:34:42

namespace Vitess\Proto\Vtgate {

  class SplitQueryRequest extends \DrSlump\Protobuf\Message {

    /**  @var \Vitess\Proto\Vtrpc\CallerID */
    public $caller_id = null;
    
    /**  @var string */
    public $keyspace = null;
    
    /**  @var \Vitess\Proto\Query\BoundQuery */
    public $query = null;
    
    /**  @var string */
    public $split_column = null;
    
    /**  @var int */
    public $split_count = null;
    

    /** @var \Closure[] */
    protected static $__extensions = array();

    public static function descriptor()
    {
      $descriptor = new \DrSlump\Protobuf\Descriptor(__CLASS__, 'vtgate.SplitQueryRequest');

      // OPTIONAL MESSAGE caller_id = 1
      $f = new \DrSlump\Protobuf\Field();
      $f->number    = 1;
      $f->name      = "caller_id";
      $f->type      = \DrSlump\Protobuf::TYPE_MESSAGE;
      $f->rule      = \DrSlump\Protobuf::RULE_OPTIONAL;
      $f->reference = '\Vitess\Proto\Vtrpc\CallerID';
      $descriptor->addField($f);

      // OPTIONAL STRING keyspace = 2
      $f = new \DrSlump\Protobuf\Field();
      $f->number    = 2;
      $f->name      = "keyspace";
      $f->type      = \DrSlump\Protobuf::TYPE_STRING;
      $f->rule      = \DrSlump\Protobuf::RULE_OPTIONAL;
      $descriptor->addField($f);

      // OPTIONAL MESSAGE query = 3
      $f = new \DrSlump\Protobuf\Field();
      $f->number    = 3;
      $f->name      = "query";
      $f->type      = \DrSlump\Protobuf::TYPE_MESSAGE;
      $f->rule      = \DrSlump\Protobuf::RULE_OPTIONAL;
      $f->reference = '\Vitess\Proto\Query\BoundQuery';
      $descriptor->addField($f);

      // OPTIONAL STRING split_column = 4
      $f = new \DrSlump\Protobuf\Field();
      $f->number    = 4;
      $f->name      = "split_column";
      $f->type      = \DrSlump\Protobuf::TYPE_STRING;
      $f->rule      = \DrSlump\Protobuf::RULE_OPTIONAL;
      $descriptor->addField($f);

      // OPTIONAL INT64 split_count = 5
      $f = new \DrSlump\Protobuf\Field();
      $f->number    = 5;
      $f->name      = "split_count";
      $f->type      = \DrSlump\Protobuf::TYPE_INT64;
      $f->rule      = \DrSlump\Protobuf::RULE_OPTIONAL;
      $descriptor->addField($f);

      foreach (self::$__extensions as $cb) {
        $descriptor->addField($cb(), true);
      }

      return $descriptor;
    }

    /**
     * Check if <caller_id> has a value
     *
     * @return boolean
     */
    public function hasCallerId(){
      return $this->_has(1);
    }
    
    /**
     * Clear <caller_id> value
     *
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function clearCallerId(){
      return $this->_clear(1);
    }
    
    /**
     * Get <caller_id> value
     *
     * @return \Vitess\Proto\Vtrpc\CallerID
     */
    public function getCallerId(){
      return $this->_get(1);
    }
    
    /**
     * Set <caller_id> value
     *
     * @param \Vitess\Proto\Vtrpc\CallerID $value
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function setCallerId(\Vitess\Proto\Vtrpc\CallerID $value){
      return $this->_set(1, $value);
    }
    
    /**
     * Check if <keyspace> has a value
     *
     * @return boolean
     */
    public function hasKeyspace(){
      return $this->_has(2);
    }
    
    /**
     * Clear <keyspace> value
     *
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function clearKeyspace(){
      return $this->_clear(2);
    }
    
    /**
     * Get <keyspace> value
     *
     * @return string
     */
    public function getKeyspace(){
      return $this->_get(2);
    }
    
    /**
     * Set <keyspace> value
     *
     * @param string $value
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function setKeyspace( $value){
      return $this->_set(2, $value);
    }
    
    /**
     * Check if <query> has a value
     *
     * @return boolean
     */
    public function hasQuery(){
      return $this->_has(3);
    }
    
    /**
     * Clear <query> value
     *
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function clearQuery(){
      return $this->_clear(3);
    }
    
    /**
     * Get <query> value
     *
     * @return \Vitess\Proto\Query\BoundQuery
     */
    public function getQuery(){
      return $this->_get(3);
    }
    
    /**
     * Set <query> value
     *
     * @param \Vitess\Proto\Query\BoundQuery $value
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function setQuery(\Vitess\Proto\Query\BoundQuery $value){
      return $this->_set(3, $value);
    }
    
    /**
     * Check if <split_column> has a value
     *
     * @return boolean
     */
    public function hasSplitColumn(){
      return $this->_has(4);
    }
    
    /**
     * Clear <split_column> value
     *
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function clearSplitColumn(){
      return $this->_clear(4);
    }
    
    /**
     * Get <split_column> value
     *
     * @return string
     */
    public function getSplitColumn(){
      return $this->_get(4);
    }
    
    /**
     * Set <split_column> value
     *
     * @param string $value
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function setSplitColumn( $value){
      return $this->_set(4, $value);
    }
    
    /**
     * Check if <split_count> has a value
     *
     * @return boolean
     */
    public function hasSplitCount(){
      return $this->_has(5);
    }
    
    /**
     * Clear <split_count> value
     *
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function clearSplitCount(){
      return $this->_clear(5);
    }
    
    /**
     * Get <split_count> value
     *
     * @return int
     */
    public function getSplitCount(){
      return $this->_get(5);
    }
    
    /**
     * Set <split_count> value
     *
     * @param int $value
     * @return \Vitess\Proto\Vtgate\SplitQueryRequest
     */
    public function setSplitCount( $value){
      return $this->_set(5, $value);
    }
  }
}
