'use strict';

/* Services */

var manageServices = angular.module('manageServices', ['ngResource']);

manageServices.factory('ManageTree', ['$resource',
    function ($resource) {
        return $resource('../static/manage/partials/tree.json', {}, {
            query: {method: 'GET', isArray: true}
        });
    }]);

manageServices.factory('DataRiver', ['$resource',
    function ($resource) {
        return $resource('datarivers/:name', {name: '@name'}, {
            query: {method: 'GET', isArray: true, params: {name: ""}},
            'get': {method: 'GET', isArray: true},
            'save': {method: 'POST'},
            'update': {method: 'PUT'},
            'delete': {method: 'DELETE'}
        });
    }]);

manageServices.factory('DataRiverTemplate', ['$resource',
    function ($resource) {
        return $resource('../static/manage/partials/datariver/datariver_template.json', {}, {
            query: {method: 'GET', isArray: false}
        });
    }]);

manageServices.factory('DataRiverFormat', [function () {
    return {
        /**
         * 将触发器过滤条件设置表格中的数据转化为DataRiver模型数据
         * @param grid_filter_list
         */
        get_notification_filter_field_obj: function (grid_filter_list) {
            var model_filter_list = []
            if (!grid_filter_list || grid_filter_list.length == 0) {
                return model_filter_list
            }

            for (var index in grid_filter_list) {
                var grid_filter = grid_filter_list[index]
                var model_filter = angular.copy(grid_filter)
                delete model_filter.$$hashKey
                model_filter_list.push(model_filter)
            }
            return model_filter_list
        },

        /**
         * 将变量解析配置由对象转换为数组
         * @param data_parser_cfg
         */
        get_notification_data_parser_fields_list: function (data_parser_cfg) {
            var field_cfg_lists = []
            if (!data_parser_cfg || !data_parser_cfg.fields) {
                return field_cfg_lists
            }
            angular.forEach(data_parser_cfg.fields, function (value, key) {
                var field_cfg = {}
                if (angular.isString(value)) {
                    field_cfg = {'field_name': key, 'expression': value, 'type': 'regex'}
                    this.push(field_cfg)
                }
                else {
                    field_cfg = angular.copy(value)
                    field_cfg.field_name = key
                    this.push(field_cfg)
                }
            }, field_cfg_lists)
            return field_cfg_lists
        }
        ,
        /**
         * 将变量解析器数组转换为DataRiver对象
         * @param grid_fields_list
         * @returns {{}}
         */
        get_notification_data_parser_fields_obj: function (grid_fields_list) {
            var field_cfg_obj = {}
            if (!grid_fields_list) {
                return field_cfg_obj
            }
            angular.forEach(grid_fields_list, function (value, key) {
                var field_cfg = angular.copy(value)
                var field_name = field_cfg.field_name
                delete field_cfg.field_name
                this[field_name] = field_cfg
            }, field_cfg_obj)
            return {type: 'regex', 'fields': field_cfg_obj}
        }
        ,
        /**
         * 将source request body 由对象转为数组
         * @param body_cfg
         * @returns {Array}
         */
        get_source_request_body_list: function (body_cfg) {
            var field_cfg_lists = []
            if (!body_cfg) {
                return field_cfg_lists
            }
            angular.forEach(body_cfg, function (value, key) {
                var field_cfg = {'field_name': key, 'field_value': value}
                this.push(field_cfg)
            }, field_cfg_lists)
            return field_cfg_lists
        }
        ,
        /**
         * 将source request body由数组转换为对象
         * @param field_cfg_list
         * @returns {{}}
         */
        get_source_request_body_obj: function (field_cfg_list) {
            var field_cfg_obj = {}
            if (!field_cfg_list) {
                return field_cfg_obj
            }
            angular.forEach(field_cfg_list, function (value, key) {
                this[value['field_name']] = value['field_value']
            }, field_cfg_obj)
            return field_cfg_obj
        }
        ,
        /**
         * 获取所有可供使用的变量
         * @param field_config
         * @returns {{field_value: string}[]}
         */
        get_all_select_vars: function (field_config) {
            var field_cfg_lists = [{'field_value': '{version}'}]
            if (!field_config) {
                return field_cfg_lists
            }
            angular.forEach(field_config, function (value, key) {
                var field_item = {'field_value': '{' + key + '}'}
                this.push(field_item)
            }, field_cfg_lists)
            return field_cfg_lists
        },
        /**
         * 将数据终点设置表格中的数据转化为DataRiver模型数据
         * @param grid_destination_list
         */
        get_destination_model_obj: function (grid_destination_list) {
            var model_destination_list = []
            if (!grid_destination_list || grid_destination_list.length == 0) {
                return model_destination_list
            }

            for (var index in grid_destination_list) {
                var grid_destination = grid_destination_list[index]
                var model_destination = angular.copy(grid_destination)
                delete model_destination.$$hashKey
                if (!model_destination.host) {
                    delete model_destination.host
                }
                if (!model_destination.index) {
                    delete model_destination.index
                }
                if (!model_destination.type) {
                    delete model_destination.type
                }
                if (!model_destination.id) {
                    delete model_destination.id
                }
                if (!model_destination.mapping) {
                    delete model_destination.mapping
                }
                if (!model_destination.clear_policy) {
                    delete model_destination.clear_policy
                }
                model_destination_list.push(model_destination)
            }
            return model_destination_list
        }
        ,
        /**
         * 格式化data river，部分配置存在简写和默认值的情况，修改时无法正确显示到界面
         * @param data_river
         */
        format_data_river_to_display: function (data_river) {
            var notification = data_river.notification
            if (notification) {
                if (notification.filter) {
                    if (notification.filter.conditions) {
                        for (var index in notification.filter.conditions) {
                            if (!notification.filter.conditions[index].operator) {
                                notification.filter.conditions[index].operator = 'is'
                            }
                        }
                    }
                }
            }
            var destinations = data_river.destination
            if (destinations) {
                for (var index in destinations) {
                    var destination = destinations[index]
                    if (!destination.destination_type) {
                        destination.destination_type = 'elasticsearch'
                    }
                    if (!destination.host) {
                        destination.host = ''
                    }
                    if (!destination.index) {
                        destination.index = ''
                    }
                    if (!destination.type) {
                        destination.type = ''
                    }
                    if (!destination.id) {
                        destination.id = ''
                    }
                    if (!destination.clear_policy) {
                        destination.clear_policy = ''
                    }
                }
            }
            return data_river
        },
        /**
         * 从界面的model获取最终要保存的数据
         * @param mirror_river
         * @returns {{}}
         */
        get_data_from_view: function (mirror_river) {
            if (!mirror_river.notification.filter) {
                mirror_river.notification.filter = {}
            }
            mirror_river.notification.filter.conditions = this.get_notification_filter_field_obj(
                mirror_river.filter_options.data)

            mirror_river.notification.data_parser = this.get_notification_data_parser_fields_obj(
                mirror_river.filter_data_parser_fields.data)
            if (!mirror_river.source.request) {
                mirror_river.source.request = {}
            }
            mirror_river.source.request.body = this.get_source_request_body_obj(
                mirror_river.source_request_body.data)

            if (!mirror_river.source.response) {
                mirror_river.source.response = {}
            }
            mirror_river.source.response.fields = this.get_source_request_body_obj(
                mirror_river.source_response_body.data)

            mirror_river.destination = this.get_destination_model_obj(
                mirror_river.destination_list_grid.data)

            var copy_river = {}
            copy_river.name = mirror_river.name
            copy_river.notification = mirror_river.notification
            copy_river.source = mirror_river.source
            copy_river.destination = mirror_river.destination
            return copy_river
        }
    }
}])

manageServices.factory('AlertService', ['$rootScope', function ($rootScope) {
    var alertService = {};

    // 创建一个全局的 alert 数组
    $rootScope.alerts = [];

    alertService.add = function (type, msg) {
        var alert = {
            'type': type, 'msg': msg, 'close': function () {
                alertService.closeAlert(this);
            }
        }
        for (var index in $rootScope.alerts) {
            if (angular.equals(alert, $rootScope.alerts[index]))
                return
        }
        $rootScope.alerts.push(alert);
    }
    ;

    alertService.closeAlert = function (alert) {
        alertService.closeAlertIdx($rootScope.alerts.indexOf(alert));
    };

    alertService.closeAlertIdx = function (index) {
        $rootScope.alerts.splice(index, 1);
    };

    return alertService;
}
])

manageServices.factory('EsTmpl', ['$resource',
    function ($resource) {
        return $resource('estmpls/:name', {name: '@name'}, {
            query: {method: 'GET', isArray: true, params: {name: ""}},
            'get': {method: 'GET', isArray: true},
            'save': {method: 'POST'},
            'update': {method: 'PUT'},
            'delete': {method: 'DELETE'}
        });
    }]);
manageServices.factory('EsTmplTemplate', ['$resource',
    function ($resource) {
        return $resource('../static/manage/partials/estmpl/estmpl_template.json', {}, {
            query: {method: 'GET', isArray: false}
        });
    }]);


manageServices.factory('QueryHandler', ['$resource',
    function ($resource) {
        return $resource('querychains/:name', {name: '@name'}, {
            query: {method: 'GET', isArray: true, params: {name: ""}},
            'get': {method: 'GET', isArray: true},
            'save': {method: 'POST'},
            'update': {method: 'PUT'},
            'delete': {method: 'DELETE'}
        });
    }]);

manageServices.factory('QueryHandlerTemplate', ['$resource',
    function ($resource) {
        return $resource('../static/manage/partials/queryhandler/queryhandler_template.json', {}, {
            query: {method: 'GET', isArray: false}
        });
    }]);

manageServices.factory('QueryHandlerFormat', ['QueryHandler', function (QueryHandler) {
    return {
        /**
         * 将查询处理器数据转换为适合前台显示的view model
         * @param query_handler
         */
        format_query_handler_to_display: function (query_handler) {
            if (!query_handler) {
                query_handler = {}
            }

            if (!query_handler.filter) {
                query_handler.filter = {}
            }

            if (!query_handler.filter.conditions) {
                query_handler.filter.conditions = []
            }

            if (query_handler.response) {
                query_handler.response_jsonstr = angular.toJson(query_handler.response)
            }
            else {
                query_handler.response_jsonstr = ''
            }

            if (query_handler.data_parser) {
                query_handler.data_parser_list = this.get_data_parser_fields_list(query_handler.data_parser)
            }
            else {
                query_handler.data_parser_list = []
            }

            query_handler.destination_list = this.get_destination_list(query_handler.destination)

            query_handler.http_method_view_model = this.get_http_method_view_model(query_handler.http_method)
            return query_handler
        },
        /**
         * 将前台view model转换为后台存储数据
         * @param view_query_handler
         * @returns {*}
         */
        format_query_handler_to_model: function (view_query_handler) {
            if (!view_query_handler) {
                return {}
            }

            var query_handler = new QueryHandler(angular.copy(view_query_handler))
            query_handler.response = angular.fromJson(view_query_handler.response_jsonstr)
            delete query_handler.response_jsonstr
            query_handler.destination = this.get_destination_obj(query_handler.destination_list)
            delete query_handler.destination_list
            query_handler.data_parser = this.get_data_parser_fields_obj(query_handler.data_parser_list)
            delete query_handler.data_parser_list
            query_handler.http_method = this.get_http_method_model(query_handler.http_method_view_model)
            delete query_handler.http_method_view_model
            return query_handler
        },
        /**
         * 将变量解析配置由对象转换为数组
         * @param data_parser_cfg
         */
        get_data_parser_fields_list: function (data_parser_cfg) {
            var field_cfg_lists = []
            if (!data_parser_cfg || !data_parser_cfg.fields) {
                return field_cfg_lists
            }
            angular.forEach(data_parser_cfg.fields, function (value, key) {
                var field_cfg = {}
                if (angular.isString(value)) {
                    field_cfg = {'field_name': key, 'expression': value, 'type': 'regex'}
                    this.push(field_cfg)
                }
                else {
                    field_cfg = angular.copy(value)
                    field_cfg.field_name = key
                    this.push(field_cfg)
                }
            }, field_cfg_lists)
            return field_cfg_lists
        },
        /**
         * 将变量解析器数组转换为对象
         * @param grid_fields_list
         * @returns {{}}
         */
        get_data_parser_fields_obj: function (grid_fields_list) {
            var field_cfg_obj = {}
            if (!grid_fields_list) {
                return field_cfg_obj
            }
            angular.forEach(grid_fields_list, function (value, key) {
                var field_cfg = angular.copy(value)
                var field_name = field_cfg.field_name
                delete field_cfg.field_name
                this[field_name] = field_cfg
            }, field_cfg_obj)
            return {type: 'regex', 'fields': field_cfg_obj}
        },
        /**
         * 将数据仓库配置由对象转换为数组
         * @param data_parser_cfg
         */
        get_destination_list: function (destination) {
            if (!destination) {
                destination = {}
            }
            if (!destination.destination_type) {
                destination.destination_type = 'elasticsearch'
            }
            //if (!destination.host) {
            //    destination.host = ''
            //}
            //if (!destination.index) {
            //    destination.index = ''
            //}
            //if (!destination.type) {
            //    destination.type = ''
            //}
            //if (!destination.id) {
            //    destination.id = ''
            //}
            //if (!destination.reference) {
            //    destination.reference = ''
            //}

            return [destination]
        },
        /**
         * 将数据仓库配置数组转换为对象
         * @param grid_fields_list
         * @returns {{}}
         */
        get_destination_obj: function (destination_list) {
            var field_cfg_obj = {}
            if (!destination_list || destination_list.length == 0) {
                return field_cfg_obj
            }
            var model_destination = angular.copy(destination_list[0])
            delete model_destination.$$hashKey
            if (!model_destination.host) {
                delete model_destination.host
            }
            if (!model_destination.index) {
                delete model_destination.index
            }
            if (!model_destination.type) {
                delete model_destination.type
            }
            if (!model_destination.id) {
                delete model_destination.id
            }
            if (!model_destination.mapping) {
                delete model_destination.mapping
            }
            if (!model_destination.clear_policy) {
                delete model_destination.clear_policy
            }
            return model_destination
        },
        /**
         * 将http method 字符串转化为view model
         * @param http_method_str
         * @returns {{GET: boolean, POST: boolean, DELETE: boolean, PUT: boolean}}
         */
        get_http_method_view_model: function (http_method_str) {
            var view_model = {'GET': false, 'POST': false, 'DELETE': false, 'PUT': false}
            if (!http_method_str)
                return view_model
            var method_list = http_method_str.split(',')

            for (var key in view_model) {
                if (method_list.indexOf(key) > -1) {
                    view_model[key] = true
                }
            }

            return view_model
        },
        /**
         * 将http method view model转换为后台数据字符串
         * @param view_model
         */
        get_http_method_model: function (view_model) {
            var http_method_str = ''
            for (var key in view_model) {
                if (view_model[key]) {
                    http_method_str += key + ','
                }
            }
            if (http_method_str.length > 0) {
                http_method_str = http_method_str.slice(0, http_method_str.length - 1)
            }
            return http_method_str
        }
    }
}])

manageServices.factory('SysParamTemplate', ['$resource',
    function ($resource) {
        return $resource('../static/manage/partials/sysparam/sysparam.template.json', {}, {
            query: {method: 'GET', isArray: false}
        });
    }]);

manageServices.factory('SysParam', ['$resource',
    function ($resource) {
        return $resource('sysparams', {}, {
            query: {method: 'GET', isArray: false},
            'update': {method: 'POST'}
        });
    }]);

manageServices.factory('SysParamFormat', ['SysParam', function (SysParam) {
    return {}
}]);

manageServices.factory('Process', ['$resource',
    function ($resource) {
        return $resource('processes/:host', {}, {
            'query': {method: 'GET', isArray: true}
        });
    }]);

manageServices.factory('ProcessAction', ['$resource',
    function ($resource) {
        return $resource('processes/action/:action/host/:host/name/:name', {
                name: '@name',
                action: '@action',
                host: '@host'
            }, {
                'do': {method: 'POST'},
                'get': {method: 'GET'}
            }
        );
    }]);
