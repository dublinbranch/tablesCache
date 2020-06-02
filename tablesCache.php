<?php

class TablesCache{

    private string $tableName;
    private int $ttl;
    private int $key;
    private $db;
    private ?PoorManMutex $lock = null;
    private array $sql;

    /**
     * TablesCache constructor.
     * @param $db a database class wrapper like
     * @param $tableName
     * @param $sql
     * @param int $ttl
     */
    public function __construct($db, $key, $tableName, $sql,$ttl = 60){
        if( ! function_exists( "sem_get" ) ){
            die( "sem_get is missing" );
        }

        $this->key = $key;
        $this->tableName = $tableName;
        $this->sql = $sql;
        $this->ttl = $ttl;
        $this->db = $db;
    }

    public function touch(bool $wait = true){
        if(!$this->lock){
            $this->lock = new PoorManMutex($this->key);
            $this->lock->setAutoUnlock();
        }
        if($wait){
            $this->lock->lock(); // this will block until the other task is done
        }else{
            if(!$this->lock->try_lock()){ //no wait
                return; //IF we are not able to lock, just return
            }
        }
        

        $singleTblName = str_replace( "AlbertoAux." , "" , $this->tableName );
        $sql = <<<EOD
SELECT
    UNIX_TIMESTAMP( `create_time` ) as `create_time`
FROM
    `INFORMATION_SCHEMA`.`TABLES`
WHERE
    `table_schema` = 'AlbertoAux' AND
    `table_name` = '{$singleTblName}'
EOD;

        $res = $this->db->getLine($sql);
        if( !is_null( $res ) ) {
            $t = time();
            $timediff = $res->create_time + $this->ttl;
            if( $t < $timediff ){
                return;
            }
        }

        //We create the ttable in any case to avoid error during the rename
        $create = "CREATE TABLE IF NOT EXISTS  {$this->tableName} AS SELECT 1" ;
        $this->db->query( $create );

        $temp = "{$this->tableName}_TEMP";
        $neu = "$this->tableName";
        $old = "{$this->tableName}_old";

        $drop = "DROP TABLE IF EXISTS {$temp}";
        $this->db->query( $drop );
        $create = "CREATE TABLE {$temp} AS {$this->sql[0]}";
        $this->db->query($create);

        array_shift($this->sql);

        foreach ($this->sql as $sql){
            $alter = "ALTER TABLE {$temp} {$sql}";
            $this->db->query($alter);
        }

        $rename = "RENAME TABLE $neu TO $old, $temp To $neu;";
        $this->db->query($rename);

        $dropold = "DROP TABLE {$old}";
        $this->db->query( $dropold );

    }

}
