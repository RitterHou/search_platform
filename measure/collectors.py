# coding=utf-8

from common.connections import EsConnectionFactory
from common.loggers import app_log
from common.exceptions import GenericError, InvalidParamError
from common.utils import deep_merge, get_time_by_mill_str
from measure.measurements import measurement_helper


__author__ = 'liuzhaoming'


class Collector(object):
    def collect(self, task_cfg, measure_objs, measurements):
        """
        数据采集接口
        :param task_cfg:
        :return:
        """
        pass


class EsStatsCollector(Collector):
    def collect(self, task_cfg, measure_objs, measurements):
        """
        通过ES Stats方式采集数据
        :param task_cfg:
        :param measure_objs:
        :return:
        """
        if not measure_objs or not task_cfg or not measurements:
            app_log.warning(
                'EsStatsCollector input param is invalid task_cfg={0} , measure_objs={1} , measurements={2}', task_cfg,
                measure_objs, measurements)
            return
        es_conn, es_index_list = self._get_es_operation_params(measure_objs)
        index_name_list = map(lambda item: item['index'], es_index_list)
        index_stats_result = {}

        es_index_stats_result = es_conn.indices.stats()
        for index_name in index_name_list:
            index_stats_info = es_index_stats_result['indices'].get(index_name)
            index_stats = {}
            index_stats_result[index_name] = index_stats
            for measurement in measurements:
                cur_value = measurement.get_sample_value(index_stats_info)
                index_stats[measurement.cfg['name']] = cur_value

        return index_stats_result

    def _get_es_operation_params(self, measure_objs):
        """
        获取ES相关参数
        :param measure_objs:
        :return:
        """
        es_hosts = measure_objs[0]['host']
        es_conn = EsConnectionFactory.get_es_connection(es_hosts)
        es_index_list = filter(lambda es_index_info: es_index_info['index'], measure_objs)
        return es_conn, es_index_list


class EsSearchCollector(Collector):
    """
    ES search 方式采集器
    """

    def collect(self, task_cfg, measure_objs, measurements):
        """
        通过Elasticsearch Search的方式采集数据
        :param task_cfg:
        :param measure_objs:
        :param measurements:
        :return:
        """
        if not measure_objs or not task_cfg or not measurements:
            app_log.warning(
                'EsSearchCollector input param is invalid task_cfg={0} , measure_objs={1} , measurements={2}', task_cfg,
                measure_objs, measurements)
            return

        es_conn, es_index_list = self._get_es_operation_params(measure_objs)

        msearch_result = self._es_msearch(es_conn, es_index_list, measurements)
        return self._parse_es_msearch_result(es_index_list, measurements, msearch_result)

    def _get_es_operation_params(self, measure_objs):
        """
        获取ES相关参数
        :param measure_objs:
        :return:
        """
        es_hosts = measure_objs[0]['host']
        es_conn = EsConnectionFactory.get_es_connection(es_hosts)
        es_index_list = filter(lambda es_index_info: es_index_info['index'], measure_objs)
        return es_conn, es_index_list


    def _es_msearch(self, es_conn, es_index_list, measurements):
        """
        执行ES msearch 查询
        :param es_conn:
        :param es_index_list:
        :param es_msearch_body:
        :param measurements:
        :return:
        """
        es_msearch_body = []
        for es_index_info in es_index_list:
            for measurement in measurements:
                measurement_dsl = measurement.get_dsl(es_index_info['index'])
                if not measurement_dsl:
                    app_log.error('Cannot get measurement dsl {0}', measurement.cfg)
                    raise GenericError('Cannot get measurement dsl')
                es_msearch_body.extend(measurement_dsl)

        msearch_result = es_conn.msearch(es_msearch_body)
        return msearch_result

    def _parse_es_msearch_result(self, es_index_list, measurements, msearch_result):
        """
        解析ES msearch count result
        :param es_index_list:
        :param measurements:
        :param msearch_result:
        :return:
        """
        index_count_result = {}
        for es_index_info in es_index_list:
            count_dict = {}
            index_count_result[es_index_info['index']] = count_dict
            for measurement in measurements:
                count_dict[measurement.cfg['name']] = measurement.get_sample_value(msearch_result['responses'].pop(0))
        return index_count_result


class EsCountCollector(Collector):
    """
    ES count 采集器, 已经废弃
    """

    def collect(self, task_cfg, measure_objs, measurements):
        """
        通过Elasticsearch count的方式采集数据, 统一使用EsSearchCollector
        :param task_cfg:
        :param measure_objs:
        :param measurements:
        :return:
        """
        if not measure_objs or not task_cfg or not measurements:
            app_log.warning(
                'EsSearchCollector input param is invalid task_cfg={0} , measure_objs={1} , measurements={2}', task_cfg,
                measure_objs, measurements)
            return

        es_conn, es_index_list = self._get_es_params(measure_objs)

        msearch_result = self._es_msearch(es_conn, es_index_list, measurements)
        return self._parse_es_msearch_result(es_index_list, measurements, msearch_result)

    def _get_es_params(self, measure_objs):
        """
        获取ES相关参数
        :param measure_objs:
        :return:
        """
        es_hosts = measure_objs[0]['hosts_str']
        es_conn = EsConnectionFactory.get_es_connection(es_hosts)
        es_index_list = filter(lambda es_index_info: es_index_info['index'], measure_objs)
        return es_conn, es_index_list


    def _es_msearch(self, es_conn, es_index_list, measurements):
        """
        执行ES msearch 查询
        :param es_conn:
        :param es_index_list:
        :param es_msearch_body:
        :param measurements:
        :return:
        """
        es_msearch_body = []
        for es_index_info in es_index_list:
            for measurement in measurements:
                self._get_es_msearch_dsl(es_index_info, measurement, es_msearch_body)
        msearch_result = es_conn.msearch(es_msearch_body)
        return msearch_result

    def _parse_es_msearch_result(self, es_index_list, measurements, msearch_result):
        """
        解析ES msearch count result
        :param es_index_list:
        :param measurements:
        :param msearch_result:
        :return:
        """
        count_result_list = map(lambda es_result: es_result['hits']['total'], msearch_result)
        index_count_result = {}
        for es_index_info in es_index_list:
            count_dict = {}
            index_count_result[es_index_info['index']] = count_dict
            for measurement in measurements:
                count_dict[measurement.cfg['name']] = count_result_list.pop(0)
        return index_count_result

    def _get_es_msearch_dsl(self, es_index_info, measurement, es_msearch_body):
        """
        获取ES msearch dsl
        :param es_index_info:
        :param measurement:
        :param es_msearch_body:
        :return:
        """
        if measurement.fingerprint == 'type=elasticsearch, operation=search, search_type=count':
            es_msearch_body.extend(({"search_type": "count", "index": es_index_info['index']},
                                    measurement.cfg.get('dsl') if measurement.cfg.get('dsl') else {
                                        "query": {"match_all": {}}}))
        else:
            app_log.error('Invalid measurement config {0}', measurement.cfg)
            raise GenericError('Invalid measurement config')


class EsAggregationCollector(EsCountCollector):
    """
    ES aggregation 采集器, 已经废弃，统一使用EsSearchCollector
    """

    def _get_es_msearch_dsl(self, es_index_info, measurement, es_msearch_body):
        """
        获取ES msearch dsl
        :param es_index_info:
        :param measurement:
        :param es_msearch_body:
        :return:
        """
        if measurement.fingerprint == 'type=elasticsearch, operation=search, search_type=aggs':
            agg_dsl = measurement.cfg.get('dsl') if measurement.cfg.get('dsl') else None
            if 'size' not in agg_dsl:
                agg_dsl['size'] = 0
            es_msearch_body.extend(({"index": es_index_info['index']},
                                    measurement.cfg.get('dsl') if measurement.cfg.get('dsl') else {
                                        "query": {"match_all": {}}}))
        else:
            app_log.error('Invalid measurement config {0}', measurement.cfg)
            raise GenericError('Invalid measurement config')

    def _parse_es_msearch_result(self, es_index_list, measurements, msearch_result):
        """
        解析ES msearch aggs result
        :param es_index_list:
        :param measurements:
        :param msearch_result:
        :return:
        """
        count_result_list = map(lambda es_result: es_result['hits']['total'], msearch_result)
        index_count_result = {}
        for es_index_info in es_index_list:
            count_dict = {}
            index_count_result[es_index_info['index']] = count_dict
            for measurement in measurements:
                count_dict[measurement.cfg['name']] = count_result_list.pop(0)
        return index_count_result


class CollectorHelper(object):
    __es_search_collector = EsSearchCollector()
    __es_stats_collector = EsStatsCollector()

    def collect(self, task_cfg, measure_objs):
        """
        采集测量数据
        :param task_cfg:
        :param measure_objs:
        :return:
        """
        measurements_dict = measurement_helper.get_measurements_by_fingerprint(task_cfg)
        start_time = get_time_by_mill_str()
        collect_result = {}
        for measurement_fingerprint in measurements_dict:
            _collector = self.__get_collector_by_fingerprint(measurement_fingerprint)
            cur_collect_result = _collector.collect(task_cfg, measure_objs, measurements_dict[measurement_fingerprint])
            collect_result = deep_merge(collect_result, cur_collect_result)
        for measure_obj_key in collect_result:
            collect_result[measure_obj_key]['@collect_time'] = start_time

        return collect_result


    def __get_collector_by_fingerprint(self, fingerprint):
        """
        根据测量指标的指纹获取数据采集器
        :param fingerprint:
        :return:
        """
        if fingerprint == 'type=elasticsearch, operation=stats, search_type=':
            return self.__es_stats_collector
        elif fingerprint == 'type=elasticsearch, operation=search, search_type=count':
            return self.__es_search_collector
        elif fingerprint.startswith('type=elasticsearch, operation=search, search_type=aggs'):
            return self.__es_search_collector
        else:
            app_log.error("Invalid measurement fingerprint {0}", fingerprint)
            raise InvalidParamError("Invalid measurement fingerprint")


collector_helper = CollectorHelper()

if __name__ == '__main__':
    es_connection = EsConnectionFactory.get_es_connection('http://172.19.65.66:9200')
    msearch_list = [{"search_type": "count", "index": "search_platform-gonghuo-1.0.0"},
                    {"query": {"match_all": {}}}, {"index": "search_platform-gonghuo-1.0.0"},
                    {"query": {"match_all": {}}}, {"index": "search_platform-gonghuo-1.0.0"},
                    {"aggs": {"grades_count": {
                        "cardinality": {
                            "field": "spuId"
                        }
                    }}}]
    es_connection.msearch(msearch_list)

