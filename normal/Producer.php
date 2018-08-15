<?php
/**
 * Created by PhpStorm.
 * User: JV
 * Date: 2017/12/22
 * Time: 15:15
 * Author: JV
 */

/**
 * Class Producer
 * @package App\MQ
 * Author: JV
 * kafka 生产者
 */
class Producer
{
    private static $_instance = null;

    private static $_init = null;
    
    private function __construct()
    {
    }

    /**
     * @return RdKafka\Conf|null
     * Author: JV
     * 初始化
     */
    public static function getInstance()
    {
        if(is_null(self::$_instance)){
            self::$_instance = new self();
        }

        if(is_null(self::$_init)){
            //此处需要配置SASL认证 2018-08-18 阿里云团队将kafka迁入vpc
	    $setting = config('mq');
            self::$_init = new \RdKafka\Conf();
            self::$_init->set('sasl.mechanisms','PLAIN');
            self::$_init->set('api.version.request','true');
            self::$_init->set('sasl.username',$setting['sasl_plain_username']);
            self::$_init->set('sasl.password',$setting['sasl_plain_password']);
            self::$_init->set('security.protocol','SASL_SSL');
            self::$_init->set('ssl.ca.location',base_path().'/ca-cert');
            self::$_init->set('message.send.max.retries',5);
        }

        return self::$_instance;
    }

    /**
     * @param array $data
     * @param string
     * @return bool
     * Author: JV
     * send 方法
     */
    public function send(array $data,string $key,array $config=[]):bool
    {
        try{
            $setting = empty($config) ? config('mq') : $config;
            $send = new \RdKafka\Producer(self::$_init);
            $send->setLogLevel(LOG_DEBUG);
            $send->addBrokers($setting['bootstrap_servers']);
            $topic = $send->newTopic($setting['topic_name']);
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($data),$key);
            $send->poll(0);
            while($send->getOutQlen() > 0){
                $send->poll(50);
            }
	    // todo something
        }catch(\Exception $e){
            // log
	}
        return true;
    }

}
