<?php
/**
 * Created by PhpStorm.
 * User: JV
 * Date: 2017/12/22
 * Time: 15:15
 * Author: JV
 */



/**
 * Class Consumer 
 * @package App\MQ
 * Author: JV
 * kafka 生产者
 */
class Consumer
{
    private static $_instance = null;

    private static $_init = null;

    private static $_config = [];

    private function __construct()
    {
    }

    /**
     * @param array $config
     * @return RdKafka\Conf|null
     * Author: JV
     * 初始化
     */
    public static function getInstance(array $config)
    {
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        self::$_config = $config;
        if (is_null(self::$_init)) {
            // 这边需要配置认证  阿里云团队kafka 2018-08-18之后会迁入vpc环境 
	    $setting = self::$_config;
            self::$_init = new \RdKafka\Conf();
            self::$_init->set('sasl.mechanisms', 'PLAIN');
            self::$_init->set('api.version.request', 'true');
            self::$_init->set('sasl.username', $setting['sasl_plain_username']);
            self::$_init->set('sasl.password', $setting['sasl_plain_password']);
            self::$_init->set('security.protocol', 'SASL_SSL');
            self::$_init->set('ssl.ca.location', base_path() . '/ca-cert');
            self::$_init->set('group.id', $setting['consumer_id']);
            self::$_init->set('metadata.broker.list', $setting['bootstrap_servers']);
        }

        return self::$_instance;
    }

    /**
     * @param string
     * @return bool
     * Author: JV
     * send 方法
     */
    public function consumer($handle)
    {
        try {
            $setting = self::$_config;
            $topicConf = new \RdKafka\TopicConf();
            $topicConf->set('auto.offset.reset', 'smallest');
            self::$_init->setDefaultTopicConf($topicConf);
            $consumer = new \RdKafka\KafkaConsumer(self::$_init);
            $consumer->subscribe([$setting['topic_name']]);

            while (true) {

                $message = $consumer->consume(120 * 1000);

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        echo "get message success\n";
                        var_dump($message);
                        $handle->handle($message->payload);
                        echo "handle message success";
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "No more messages; will wait for more\n";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "Timed out\n";
                        break;
                    default:
			// err 30 情况下 查看consumer是否创建
                        throw new \Exception($message->errstr(), $message->err);
                        break;
                }

            }

        } catch (\Exception $e) {
            var_dump($e->getMessage(), 'mq测试失败', $e->getLine());
        }

    }

}
