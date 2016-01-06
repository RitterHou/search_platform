# coding=utf-8
import imp

import os

from common.loggers import debug_log, app_log
from search_platform.settings import BASE_DIR


__author__ = 'liuzhaoming'

# 反射出来的对象的缓存
python_reflected_obj_cache = {}
# 动态加载的模块列表，不需要重复加载模块
python_module_cache = {}


class PythonScript(object):
    """
    python脚本调用、执行类
    """

    @debug_log.debug('PythonScript.invoke')
    def invoke(self, script_cfg, input_params):
        """
        根据给定的产生调用函数
        :param script_cfg:
        :param src_obj:
        :return:
        """
        if 'obj_path' not in script_cfg:
            app_log.info('Invoke function input param is invalid')
            return
        reflected_object = self.reflect_obj(script_cfg['obj_path'])
        if isinstance(input_params, dict):
            return reflected_object(**input_params)
        else:
            return reflected_object(input_params)

    def get_module(self, module_path):
        """
        获取模块，只加载一次，不重复加载
        :param module_path:
        :return:
        """
        if module_path not in python_module_cache:
            module_name, ext = os.path.splitext(os.path.basename(module_path))
            module = imp.load_module(module_name, open(module_path), module_path, ('.py', 'U', 1))
            python_module_cache[module_path] = module
        return python_module_cache[module_path]

    @debug_log.debug('PythonScript.reflect_obj')
    def reflect_obj(self, obj_path, cached=True):
        """
        根据指定的类路径反射出对象
        :param obj_path: 类路径
        :param cached: 对象是否缓存，如果缓存的话，就类似于Spring IOC的单例，下次不会重新创建
        :return:
        """
        if not obj_path:
            return

        if obj_path in python_reflected_obj_cache:
            return python_reflected_obj_cache[obj_path]

        temp_list = obj_path.split('.')
        sub_file_name = '/'.join(temp_list[:-1]) + '.py'
        obj_name = temp_list[-1]
        module_path = BASE_DIR + '/script/python_script/' + sub_file_name

        module = self.get_module(module_path)
        obj = getattr(module, obj_name)
        if cached:
            python_reflected_obj_cache[obj_path] = obj
        return obj


python_invoker = PythonScript()

if __name__ == '__main__':
    func = python_invoker.reflect_obj('test.script_data_parsers.parse_skuids_from_pc_mq_msg')
    print func('21182:g4903:g4943;21188:g4951;21224:g4989;')