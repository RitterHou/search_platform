'use strict';

/* Controllers */

var manageControllers = angular.module('manageControllers', []);

manageControllers.controller('ManageTreeCtrl', ['$scope', 'ManageTree',
    function ($scope, ManageTree) {
        $scope.groups = ManageTree.query();
        $scope.selectedItem = '';
        $scope.def_styles = {}
        $scope.def_text_styles = {}
        $scope.options = {};

        $scope.setStyle = function (id) {
            for (var key in $scope.def_styles) {
                $scope.def_styles[key] = {}
                $scope.def_text_styles[key] = {}
            }
            $scope.def_styles[id] = {
                'z-index': 2,
                color: '#fff',
                'background-color': '#337ab7',
                'border-color': '#337ab7'
            }

            $scope.def_text_styles[id] = {color: 'black'}
        }

        $scope.setStyle('101')

    }]);

manageControllers.controller('DataRiverCtrl', ['$scope', '$state', 'DataRiverTemplate', 'DataRiver',
    function ($scope, $state, DataRiverTemplate, DataRiver) {
        $scope.selectedItem = {};
        $scope.tabs = []
        $scope.template_data = DataRiverTemplate.query()
        $scope.rivers = DataRiver.query();
    }]);

manageControllers.controller('DataRiverListCtrl', ['$scope', '$state', '$modal', 'DataRiver', '$log', 'AlertService',
    function ($scope, $state, $modal, DataRiver, $log, AlertService) {
        $scope.selectedItem = {};
        $scope.riverTableColumnDefinition = [
            {
                columnHeaderDisplayName: '数据流名称',
                displayProperty: 'name',
                sortKey: 'name',
                columnSearchProperty: 'name',
                visible: true
            },
            {
                columnHeaderTemplate: '<span><i class="glyphicon glyphicon-calendar"></i> 触发器</span>',
                //template: '<strong>{{ item.notification }}</strong>',
                sortKey: 'notification',
                //width: '12em',
                displayProperty: 'notification',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'notification'
            },
            {
                columnHeaderTemplate: '<span><i class="glyphicon glyphicon-usd"></i> 数据源</span>',
                displayProperty: 'source',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'source',
                //width: '9em',

                columnSearchProperty: 'source'
            },
            {
                columnHeaderTemplate: '<span><i class="glyphicon glyphicon-usd"></i> 数据终点</span>',
                displayProperty: 'destination',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'destination',
                //width: '9em',
                columnSearchProperty: 'destination'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)"><span class="glyphicon glyphicon-edit">' +
                '</span></button> <button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)">' +
                '<span class="glyphicon glyphicon-remove"></span></button>',
                width: '5em'
            }
        ];

        $scope.create = function (item) {
            //$scope.model = {tables: []};
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '创建数据流') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了数据流创建窗口')
                return
            }
            $scope.tabs.push({"title": '创建数据流', "source_item": null});
        };

        $scope.edit = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if (index >= 0 && $scope.tabs[index].title != '创建数据流') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了数据流编辑窗口')
                return
            }
            $scope.tabs.push({"title": item.name, "source_item": item});
        };

        $scope.remove = function (item) {
            var cur_data_river = new DataRiver(item)
            cur_data_river.$delete({}, function () {
                for (var index = 0; index < $scope.rivers.length; index++) {
                    var cur_river = $scope.rivers[index];
                    if (cur_river == item) {
                        break;
                    }
                }
                $scope.tmpls.splice(index, 1)
            })
        };
    }]);

manageControllers.controller('DataRiverEditCtrl', ['$scope', '$state', '$modal', 'DataRiverFormat', 'DataRiver',
    function ($scope, $state, $modal, DataRiverFormat, DataRiver) {
        $scope.source_river = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_river = $scope.source_river ? DataRiverFormat.format_data_river_to_display(
            angular.copy($scope.source_river)) : {}
        $scope.op_type = $scope.source_river ? 'edit' : 'create'
        $scope.$on('$viewContentLoaded', function () {
            $state.go('node.101.info.edit');
        });
        $scope.get_active_tab = function () {
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].active) {
                    return $scope.tabs[index]
                }
            }
        }
        $scope.close_tab = function () {
            var active_tab = $scope.get_active_tab()
            delete_array_element(active_tab, $scope.tabs)
        }
        $scope.save_data_river = function () {
            if (!$scope.mirror_river.notification.filter) {
                $scope.mirror_river.notification.filter = {}
            }
            $scope.mirror_river.notification.filter.conditions = DataRiverFormat.get_notification_filter_field_obj(
                $scope.mirror_river.filter_options.data)

            $scope.mirror_river.notification.data_parser = DataRiverFormat.get_notification_data_parser_fields_obj(
                $scope.mirror_river.filter_data_parser_fields.data)
            if (!$scope.mirror_river.source.request) {
                $scope.mirror_river.source.request = {}
            }
            $scope.mirror_river.source.request.body = DataRiverFormat.get_source_request_body_obj(
                $scope.mirror_river.source_request_body.data)

            if (!$scope.mirror_river.source.response) {
                $scope.mirror_river.source.response = {}
            }
            $scope.mirror_river.source.response.fields = DataRiverFormat.get_source_request_body_obj(
                $scope.mirror_river.source_response_body.data)

            $scope.mirror_river.destination = DataRiverFormat.get_destination_model_obj(
                $scope.mirror_river.destination_list_grid.data)

            var copy_river = new DataRiver()
            copy_river.name = $scope.mirror_river.name
            copy_river.notification = $scope.mirror_river.notification
            copy_river.source = $scope.mirror_river.source
            copy_river.destination = $scope.mirror_river.destination

            if ($scope.op_type == 'create') {
                copy_river.$save({}, function () {
                    var cur_rivers = DataRiver.query({}, function () {
                        $scope.rivers.length = 0
                        for (var index in copy_river) {
                            $scope.rivers.push(cur_rivers[index])
                        }

                        $scope.close_tab()
                    });
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_river.$update({}, function () {
                    var cur_rivers = DataRiver.query({}, function () {
                        $scope.rivers.length = 0
                        for (var index in cur_rivers) {
                            if (cur_rivers[index].name == $scope.mirror_river.name) {
                                $scope.rivers[index] = ($scope.mirror_river)
                            }
                            else {
                                $scope.rivers[index] = (cur_rivers[index])
                            }
                        }
                        $scope.close_tab()
                    })
                })
            }
        }
    }]);

manageControllers.controller('DataRiverNotificationCtrl', ['$scope', '$state', 'DataRiverFormat', 'AlertService',
    function ($scope, $state, DataRiverFormat, AlertService) {
        $scope.notification = {}
        $scope.filter_options = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope.filter_data_parser_fields = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope.mirror_river.filter_options = $scope.filter_options
        $scope.mirror_river.filter_data_parser_fields = $scope.filter_data_parser_fields
        if ($scope.mirror_river && $scope.mirror_river.notification) {
            $scope.notification = $scope.mirror_river.notification
            $scope.filter_options.data = $scope.notification.filter.conditions;
            $scope.filter_data_parser_fields.data = DataRiverFormat.get_notification_data_parser_fields_list(
                $scope.notification.data_parser)
        }
        else {
            $scope.mirror_river.notification = $scope.notification
            $scope.filter_options.data = []
        }
        $scope.filter_options.columnDefs = [
            {
                name: 'operator',
                displayName: '逻辑运算符',
                width: '20%',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.condition_match_operator,
                editDropdownIdLabel: 'value'
            },
            {
                name: 'type',
                displayName: '匹配类型',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                width: '10%',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.condition_match_type,
                editDropdownIdLabel: 'value'
            },
            {name: 'expression', displayName: '表达式'}
        ];

        $scope.filter_data_parser_fields.columnDefs = [
            {
                name: 'field_name',
                displayName: '变量名',
                width: '20%'
            },
            {
                name: 'type',
                displayName: '匹配类型',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                width: '10%',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.data_parser_type,
                editDropdownIdLabel: 'value'
            },
            {name: 'expression', displayName: '表达式'}
        ];

        $scope.filter_options.onRegisterApi = function (gridApi) {
            $scope.filter_options.gridApi = gridApi;
        };
        $scope.delete_filter_row = function () {
            var select_rows = $scope.filter_options.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.filter_options.data)
            }
        }
        $scope.add_filter_row = function () {
            $scope.filter_options.data.push({'operator': '', 'type': '', 'expression': ''})
        }

        $scope.filter_data_parser_fields.onRegisterApi = function (gridApi) {
            $scope.filter_data_parser_fields.gridApi = gridApi;
        };
        $scope.delete_parser_row = function () {
            var select_rows = $scope.filter_data_parser_fields.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.filter_data_parser_fields.data)
            }
        }

        $scope.add_parser_row = function () {
            $scope.filter_data_parser_fields.data.push({'field_name': '', 'type': '', 'expression': ''})
        }
    }])
;

manageControllers.controller('DataRiverSourceCtrl', ['$scope', '$state', 'DataRiverFormat', 'AlertService',
    function ($scope, $state, DataRiverFormat, AlertService) {
        $scope.source = {}
        $scope['source_request_body'] = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope['source_response_body'] = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        var source_request_body = $scope['source_request_body']
        var source_response_body = $scope['source_response_body']
        $scope.mirror_river.source_request_body = source_request_body
        $scope.mirror_river.source_response_body = source_response_body
        if ($scope.mirror_river && $scope.mirror_river.source) {
            $scope.source = $scope.mirror_river.source
            source_request_body.data = DataRiverFormat.get_source_request_body_list(
                $scope.source.request.body)
            if ($scope.source.response && $scope.source.response.fields) {
                source_response_body.data = DataRiverFormat.get_source_request_body_list(
                    $scope.source.response.fields)
            }
            else {
                source_response_body.data = []
            }
        }
        else {
            $scope.mirror_river.source = $scope.source
            source_request_body.data = []
            source_response_body.data = []
        }
        source_request_body.columnDefs = [
            {
                name: 'field_name',
                displayName: '参数名',
                width: '20%'
            },
            {
                name: 'field_value',
                displayName: '绑定变量'
            }
        ];

        source_response_body.columnDefs = [
            {
                name: 'field_name',
                displayName: '参数名',
                width: '20%'
            },
            {
                name: 'field_value',
                displayName: '绑定变量'
            }
        ];

        $scope.source_request_body.onRegisterApi = function (gridApi) {
            $scope.source_request_body.gridApi = gridApi;
        };
        $scope.delete_source_request_body_row = function () {
            var select_rows = $scope.source_request_body.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.source_request_body.data)
            }
        }
        $scope.add_source_request_body_row = function () {
            $scope.source_request_body.data.push({'field_name': '', 'field_value': ''})
        }

        $scope.source_response_body.onRegisterApi = function (gridApi) {
            $scope.source_response_body.gridApi = gridApi;
        };
        $scope.delete_source_response_row = function () {
            var select_rows = $scope.source_response_body.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.source_response_body.data)
            }
        }

        $scope.add_source_response_row = function () {
            $scope.source_response_body.data.push({'field_name': '', 'field_value': ''})
        }
    }]);

manageControllers.controller('DataRiverDestinationCtrl', ['$scope', '$state', 'AlertService',
    function ($scope, $state, AlertService) {
        $scope.destination = {}
        $scope.destination_list_grid = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope.mirror_river.destination_list_grid = $scope.destination_list_grid
        if ($scope.mirror_river && $scope.mirror_river.destination) {
            $scope.destination = $scope.mirror_river.destination
            $scope.destination_list_grid.data = $scope.destination
        }
        else {
            $scope.mirror_river.destination = $scope.destination
            $scope.destination_list_grid.data = []
        }
        $scope.destination_list_grid.columnDefs = [
            {
                name: 'destination_type',
                displayName: '数据终点类型',
                width: 150,
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.destination_types,
                editDropdownIdLabel: 'value'
            },
            {
                name: 'reference',
                width: 150,
                displayName: '引用'
            },
            {
                name: 'operation',
                displayName: '操作',
                width: 150,
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.destination_operations,
                editDropdownIdLabel: 'value'
            }, {
                name: 'clear_policy',
                displayName: '数据清除',
                width: 150,
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.destination_clear_policy,
                editDropdownIdLabel: 'value'
            },
            {name: 'id', width: 150, displayName: '数据ID'},
            {name: 'host', width: 250, displayName: 'ElasticSearch服务器地址'},
            {name: 'index', width: 250, displayName: '索引名称'},
            {name: 'type', width: 150, displayName: '文档类型'},
            {name: 'mapping', width: 250, displayName: '数据映射'}
        ];
        $scope.destination_list_grid.onRegisterApi = function (gridApi) {
            $scope.destination_list_grid.gridApi = gridApi;
        };
        $scope.delete_destination_list_row = function () {
            var select_rows = $scope.destination_list_grid.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.destination_list_grid.data)
            }
        }

        $scope.add_destination_list_row = function () {
            $scope.destination_list_grid.data.push({
                'destination_type': '', 'reference': '',
                'operation': '', 'clear_policy': '', 'id': '', 'host': '', 'index': '', 'type': '', 'mapping': ''
            })
        }

    }]);

manageControllers.controller('EsTmplCtrl', ['$scope', '$state', 'EsTmplTemplate', 'EsTmpl',
    function ($scope, $state, EsTmplTemplate, EsTmpl) {
        $scope.tabs = []
        $scope.template_data = EsTmplTemplate.query()
        $scope.tmpls = EsTmpl.query();
    }]);

manageControllers.controller('EsTmplListCtrl', ['$scope', '$state', '$modal', 'EsTmpl', 'AlertService',
    function ($scope, $state, $modal, EsTmpl, AlertService) {
        $scope.tmplTableColumnDefinition = [
            {
                columnHeaderDisplayName: 'ElasticSearch模板名称',
                displayProperty: 'name',
                sortKey: 'name',
                columnSearchProperty: 'name',
                visible: true
            },
            {
                columnHeaderTemplate: '主机地址',
                sortKey: 'host',
                displayProperty: 'host',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'host'
            },
            {
                columnHeaderTemplate: '索引',
                displayProperty: 'index',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                sortKey: 'index',
                columnSearchProperty: 'index'
            },
            {
                columnHeaderTemplate: '文档类型',
                displayProperty: 'type',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                sortKey: 'type',
                columnSearchProperty: 'type'
            }, {
                columnHeaderTemplate: '文档主键',
                displayProperty: 'id',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                sortKey: 'id',
                columnSearchProperty: 'id'
            }, {
                columnHeaderTemplate: '文档映射',
                displayProperty: 'mapping',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'mapping',
                columnSearchProperty: 'mapping'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)"><span class="glyphicon glyphicon-edit">' +
                '</span></button> <button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)">' +
                '<span class="glyphicon glyphicon-remove"></span></button>',
                width: '5em'
            }
        ];

        $scope.create = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '创建ElasticSearch模板') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了ElasticSearch模板创建窗口')
                return
            }
            $scope.tabs.push({"title": '创建ElasticSearch模板', "source_item": null});
        };

        $scope.edit = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if (index >= 0 && $scope.tabs[index].title != '创建ElasticSearch模板') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了ElasticSearch模板编辑窗口')
                return
            }
            $scope.tabs.push({"title": item.name, "source_item": item});
        };

        $scope.remove = function (item) {
            var cur_es_tmpl = new EsTmpl(item)
            cur_es_tmpl.$delete({}, function () {
                for (var index = 0; index < $scope.tmpls.length; index++) {
                    var cur_river = $scope.tmpls[index];
                    if (cur_river == item) {
                        break;
                    }
                }
                $scope.tmpls.splice(index, 1)
            })
        };
    }]);

manageControllers.controller('EsTmplEditCtrl', ['$scope', '$state', 'EsTmpl',
    function ($scope, $state, EsTmpl) {
        $scope.source_tmpl = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_tmpl = $scope.source_tmpl ? angular.copy($scope.source_tmpl) : {}
        $scope.mirror_tmpl.mapping_jsonstr = angular.toJson($scope.mirror_tmpl.mapping)
        $scope.op_type = $scope.source_tmpl ? 'edit' : 'create'
        $scope.$on('$viewContentLoaded', function () {
            //$state.go('node.103.info.edit');
        });
        $scope.get_active_tab = function () {
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].active) {
                    return $scope.tabs[index]
                }
            }
        }
        $scope.close_tab = function () {
            var active_tab = $scope.get_active_tab()
            delete_array_element(active_tab, $scope.tabs)
        }
        $scope.save_es_tmpl = function () {
            var copy_tmpl = new EsTmpl()
            if ($scope.mirror_tmpl.host) {
                copy_tmpl.host = $scope.mirror_tmpl.host
            }
            if ($scope.mirror_tmpl.index) {
                copy_tmpl.index = $scope.mirror_tmpl.index
            }
            if ($scope.mirror_tmpl.type) {
                copy_tmpl.type = $scope.mirror_tmpl.type
            }
            if ($scope.mirror_tmpl.id) {
                copy_tmpl.id = $scope.mirror_tmpl.id
            }
            if ($scope.mirror_tmpl.mapping) {
                copy_tmpl.mapping = angular.toJson($scope.mirror_tmpl.mapping)
            }
            if ($scope.mirror_tmpl.name) {
                copy_tmpl.name = $scope.mirror_tmpl.name
            }
            if ($scope.op_type == 'create') {
                copy_tmpl.$save({}, function () {
                    var cur_tmpls = EsTmpl.query({}, function () {
                        $scope.tmpls.length = 0
                        for (var index in cur_tmpls) {
                            $scope.tmpls.push(cur_tmpls[index])
                        }

                        $scope.close_tab()
                    });
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_tmpl.$update({}, function () {
                    var cur_tmpls = EsTmpl.query({}, function () {
                        $scope.tmpls.length = 0
                        for (var index in cur_tmpls) {
                            if (cur_tmpls[index].name == $scope.mirror_tmpl.name) {
                                $scope.tmpls[index] = ($scope.mirror_tmpl)
                            }
                            else {
                                $scope.tmpls[index] = (cur_tmpls[index])
                            }
                        }
                        $scope.close_tab()
                    })
                })
            }
        }
    }]);


/***************************************************************************************
 *
 *                              RESTful请求过滤
 *
 **************************************************************************************/
manageControllers.controller('QueryHandlerCtrl', ['$scope', '$state', 'DataRiverTemplate', 'QueryHandler',
    function ($scope, $state, DataRiverTemplate, QueryHandler) {
        $scope.selectedItem = {};
        $scope.tabs = []
        $scope.template_data = DataRiverTemplate.query()
        $scope.handlers = QueryHandler.query();
    }]);

manageControllers.controller('QueryHandlerListCtrl', ['$scope', '$state', '$modal', 'QueryHandler', 'AlertService',
    function ($scope, $state, $modal, QueryHandler, AlertService) {

        $scope.selectedItem = {};
        $scope.queryhandlerTableColumnDefinition = [
            {
                columnHeaderDisplayName: 'RESTful接口名称',
                displayProperty: 'name',
                sortKey: 'name',
                columnSearchProperty: 'name',
                visible: true
            },
            {
                columnHeaderTemplate: '资源类型',
                sortKey: 'res_type',
                displayProperty: 'res_type',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'res_type'
            },
            {
                columnHeaderTemplate: 'HTTP操作',
                displayProperty: 'http_method',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                width: '6%',
                columnSearchProperty: 'http_method'
            },
            {
                columnHeaderTemplate: '请求过滤器',
                displayProperty: 'filter',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'filter',
                columnSearchProperty: 'filter'
            },
            {
                columnHeaderTemplate: '请求解析器',
                displayProperty: 'data_parser',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'data_parser',
                columnSearchProperty: 'data_parser'
            },
            {
                columnHeaderTemplate: '数据仓库',
                displayProperty: 'destination',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'destination',
                columnSearchProperty: 'destination'
            },
            {
                columnHeaderTemplate: '响应数据处理',
                displayProperty: 'response',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                sortKey: 'response',
                columnSearchProperty: 'response'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)"><span class="glyphicon glyphicon-edit">' +
                '</span></button> <button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)">' +
                '<span class="glyphicon glyphicon-remove"></span></button>',
                width: '5em'
            }
        ];

        $scope.create = function (item) {
            //$scope.model = {tables: []};
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '创建RESTful接口') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了RESTful接口创建窗口')
                return
            }
            $scope.tabs.push({"title": '创建RESTful接口', "source_item": null});
        };

        $scope.edit = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if (index >= 0 && $scope.tabs[index].title != '创建RESTful接口') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了RESTful接口编辑窗口')
                return
            }
            $scope.tabs.push({"title": item.name, "source_item": item});
        };

        $scope.remove = function (item) {
            var cur_query_handler = new QueryHandler(item)
            cur_query_handler.$delete({}, function () {
                for (var index = 0; index < $scope.handlers.length; index++) {
                    var cur_river = $scope.handlers[index];
                    if (cur_river == item) {
                        break;
                    }
                }
                $scope.handlers.splice(index, 1)
            })
        };
    }]);

manageControllers.controller('QueryHandlerEditCtrl', ['$scope', '$state', '$modal', 'QueryHandlerFormat', 'QueryHandler',
    function ($scope, $state, $modal, QueryHandlerFormat, QueryHandler) {
        $scope.source_handler = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_handler = QueryHandlerFormat.format_query_handler_to_display(angular.copy($scope.source_handler))
        $scope.op_type = $scope.source_handler ? 'edit' : 'create'
        $scope.$on('$viewContentLoaded', function () {
            //$state.go('node.102.info.edit');
        });
        $scope.get_active_tab = function () {
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].active) {
                    return $scope.tabs[index]
                }
            }
        }
        $scope.close_tab = function () {
            var active_tab = $scope.get_active_tab()
            delete_array_element(active_tab, $scope.tabs)
        }
        $scope.save_query_handler = function () {
            var copy_query_handler = QueryHandlerFormat.format_query_handler_to_model($scope.mirror_handler)

            if ($scope.op_type == 'create') {
                copy_query_handler.$save({}, function () {
                    var cur_handlers = QueryHandler.query({}, function () {
                        $scope.handlers.length = 0
                        for (var index in copy_query_handler) {
                            $scope.handlers.push(cur_handlers[index])
                        }

                        $scope.close_tab()
                    });
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_query_handler.$update({}, function () {
                    var cur_handlers = QueryHandler.query({}, function () {
                        $scope.handlers.length = 0
                        for (var index in cur_handlers) {
                            if (cur_handlers[index].name == $scope.mirror_handler.name) {
                                $scope.handlers[index] = ($scope.mirror_handler)
                            }
                            else {
                                $scope.handlers[index] = (cur_handlers[index])
                            }
                        }
                        $scope.close_tab()
                    })
                })
            }
        }


        $scope.filter_options = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope.data_parser_fields = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }

        $scope.filter_options.data = $scope.mirror_handler.filter.conditions;
        $scope.data_parser_fields.data = $scope.mirror_handler.data_parser_list

        $scope.filter_options.columnDefs = [
            {
                name: 'operator',
                displayName: '逻辑运算符',
                width: '20%',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.condition_match_operator,
                editDropdownIdLabel: 'value'
            },
            {
                name: 'type',
                displayName: '匹配类型',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                width: '10%',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.condition_match_type,
                editDropdownIdLabel: 'value'
            },
            {name: 'expression', displayName: '表达式'}
        ];

        $scope.data_parser_fields.columnDefs = [
            {
                name: 'field_name',
                displayName: '变量名',
                width: '20%'
            },
            {
                name: 'type',
                displayName: '匹配类型',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                width: '10%',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.data_parser_type,
                editDropdownIdLabel: 'value'
            },
            {name: 'expression', displayName: '表达式'}
        ];

        $scope.filter_options.onRegisterApi = function (gridApi) {
            $scope.filter_options.gridApi = gridApi;
        };
        $scope.delete_filter_row = function () {
            var select_rows = $scope.filter_options.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.filter_options.data)
            }
        }
        $scope.add_filter_row = function () {
            $scope.filter_options.data.push({'operator': '', 'type': '', 'expression': ''})
        }

        $scope.data_parser_fields.onRegisterApi = function (gridApi) {
            $scope.data_parser_fields.gridApi = gridApi;
        };
        $scope.delete_parser_row = function () {
            var select_rows = $scope.data_parser_fields.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.data_parser_fields.data)
            }
        }

        $scope.add_parser_row = function () {
            $scope.data_parser_fields.data.push({'field_name': '', 'type': '', 'expression': ''})
        }


        $scope.destination_list_grid = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }
        $scope.destination_list_grid.data = $scope.mirror_handler.destination_list
        $scope.destination_list_grid.columnDefs = [
            {
                name: 'destination_type',
                displayName: '数据终点类型',
                width: 150,
                editableCellTemplate: 'ui-grid/dropdownEditor',
                editDropdownValueLabel: 'display_value',
                editDropdownOptionsArray: $scope.template_data.destination_types,
                editDropdownIdLabel: 'value'
            },
            {
                name: 'reference',
                width: 150,
                displayName: '引用'
            },
            {name: 'id', width: 150, displayName: '数据ID'},
            {name: 'host', width: 250, displayName: 'ElasticSearch服务器地址'},
            {name: 'index', width: 250, displayName: '索引名称'},
            {name: 'type', width: 150, displayName: '文档类型'},
        ];
        $scope.destination_list_grid.onRegisterApi = function (gridApi) {
            $scope.destination_list_grid.gridApi = gridApi;
        };
    }]);

manageControllers.controller('SysParamCtrl', ['$scope', '$state', 'SysParamTemplate', 'SysParam', 'AlertService',
    function ($scope, $state, SysParamTemplate, SysParam, AlertService) {
        $scope.template_data = SysParamTemplate.query()
        $scope.sysparam = SysParam.query({}, function () {
            $scope.host_grid.data = $scope.sysparam.manager.hosts
        });

        $scope.host_grid = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }

        if (!$scope.sysparam.manager) {
            $scope.sysparam.manager = {}
        }
        if (!$scope.sysparam.manager.hosts) {
            $scope.sysparam.manager.hosts = []
        }

        $scope.host_grid.columnDefs = [
            {
                name: 'host',
                displayName: '主机地址'
            },
            {
                name: 'supervisor_port',
                displayName: '端口'
            },
            {
                name: 'supervisor_user',
                displayName: 'Supervisor用户名'
            }, {
                name: 'supervisor_password',
                displayName: 'Supervisor密码'
            }
        ];

        $scope.host_grid.onRegisterApi = function (gridApi) {
            $scope.host_grid.gridApi = gridApi;
        };
        $scope.delete_host_row = function () {
            var select_rows = $scope.host_grid.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
                return
            }
            else {
                delete_array_element(select_rows[0], $scope.host_grid.data)
            }
        }

        $scope.add_host_row = function () {
            $scope.host_grid.data.push({
                'host': '',
                'supervisor_port': '9001',
                'supervisor_user': '',
                'supervisor_password': ''
            })
        }

        $scope.save_sysparam = function () {
            $scope.sysparam.$update()

        }
    }]);

manageControllers.controller('ProcessCtrl', ['$scope', '$state', 'Process', 'ProcessAction',
    function ($scope, $state, Process, ProcessAction) {
        $scope.hosts = Process.query();
        $scope.doAction = function (host, action, process_name) {
            var action = new ProcessAction({host: host, action: action, name: process_name})
            action.$do()
        }
        $scope.viewLog = function (host, action, process_name) {
            var action = new ProcessAction({host: host, action: action, name: process_name})
            var log_info = action.$get({}, function () {
                console.log(log_info)
            })
        }
    }]);

/**
 * 删除数组中得元素
 * @param element
 * @param arrray
 */
function delete_array_element(element, array) {
    var element_index = -1
    for (var index in array) {
        if (element == array[index]) {
            element_index = index
            break
        }
    }
    if (element_index > -1) {
        array.splice(index, 1)
    }
}
