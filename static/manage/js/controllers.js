'use strict';

/* Controllers */

var manageControllers = angular.module('manageControllers', []);

manageControllers.controller('ManageTreeCtrl', ['$scope', 'ManageTree', '$state', '$modal', '$window',
    function ($scope, ManageTree, $state, $modal, $window) {
        $scope.groups = ManageTree.query();
        $scope.def_styles = {}
        $scope.def_text_styles = {}
        $scope.tree_model = {selected_id: ''}
        $scope.get_style = function (id) {
            if ($state.includes('node.' + id)) {
                return {
                    def_styles: {
                        'z-index': 2,
                        color: '#fff',
                        'background-color': '#337ab7',
                        'border-color': '#337ab7'
                    }, def_text_styles: {color: 'black'}
                }
            } else {
                return {
                    def_styles: {}, def_text_styles: {}
                }
            }
        }

        $scope.show_confirm = function (info, callback, size) {
            var modalInstance = $modal.open({
                animation: true,
                templateUrl: 'confirmModalContent.html',
                controller: 'ConfirmModalInstanceCtrl',
                size: size,
                backdrop: false,
                resolve: {
                    information: function () {
                        return info;
                    }
                }
            });

            modalInstance.result.then(function (boo_result) {
                callback(boo_result)
            });
        };

        $scope.show_error = function (response) {
            var msg = 'Operation fails.'
            var status_code = response.status
            if (response.data && response.data.status_code) {
                status_code = response.data.status_code
            }
            if (status_code) {
                msg += '\nStatus code: ' + status_code
            }

            var detail = response.statusText
            if (response.data && response.data.detail) {
                detail = response.data.detail
            }
            if (detail && detail.length > 0) {
                msg += '\nDetail: ' + detail
            }

            $window.alert(msg)
        };

    }
]);

manageControllers.controller('DataRiverCtrl', ['$scope', '$state', 'DataRiverTemplate', 'DataRiver',
    function ($scope, $state, DataRiverTemplate, DataRiver) {
        $scope.selectedItem = {};
        $scope.tabs = []
        $scope.template_data = DataRiverTemplate.query()
        $scope.model = {rivers: []}
        $scope.model.rivers = DataRiver.query();
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
                columnHeaderTemplate: '<span> 触发器</span>',
                //template: '<strong>{{ item.notification }}</strong>',
                //sortKey: 'notification',
                //width: '12em',
                displayProperty: 'notification',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'notification'
            },
            {
                columnHeaderTemplate: '<span>数据源</span>',
                displayProperty: 'source',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                //sortKey: 'source',
                //width: '9em',

                columnSearchProperty: 'source'
            },
            {
                columnHeaderTemplate: '<span> 数据终点</span>',
                displayProperty: 'destination',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                //sortKey: 'destination',
                //width: '9em',
                columnSearchProperty: 'destination'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)" tooltip="修改数据" tooltip-append-to-body=true><span class="glyphicon glyphicon-edit">' +
                '</span></button></div> <div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)" tooltip="删除数据" tooltip-append-to-body=true>' +
                '<span class="glyphicon glyphicon-remove"></span></button></div>',
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
            active_last($scope.tabs)
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
            active_last($scope.tabs)
        };

        $scope.remove = function (item) {
            $scope.show_confirm('你确定要删除数据吗？', function (is_confirm) {
                    if (!is_confirm) {
                        return
                    }
                    var cur_data_river = new DataRiver(item)
                    cur_data_river.$delete({}, function () {
                        for (var index = 0; index < $scope.model.rivers.length; index++) {
                            var cur_river = $scope.model.rivers[index];
                            if (cur_river == item) {
                                break;
                            }
                        }
                        $scope.model.rivers.splice(index, 1)
                    })
                }
            )
        };
    }]);

manageControllers.controller('DataRiverEditCtrl', ['$scope', '$state', '$timeout', 'DataRiverFormat', 'DataRiver',
    function ($scope, $state, $timeout, DataRiverFormat, DataRiver) {
        $scope.source_river = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_river = DataRiverFormat.format_data_river_to_display(angular.copy($scope.source_river))
        $scope.rtn_river = angular.copy($scope.mirror_river)
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
        $scope.submitted = false;
        $scope.save_data_river = function (isValid) {
            $scope.submitted = true
            if (!isValid) {
                return
            }
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

            var copy_river = new DataRiver($scope.rtn_river)
            copy_river.name = $scope.mirror_river.name
            copy_river.notification = $scope.mirror_river.notification
            copy_river.source = $scope.mirror_river.source
            copy_river.destination = $scope.mirror_river.destination

            if ($scope.op_type == 'create') {
                copy_river.$save({}, function () {
                    $timeout(function () {
                        var curTerms = DataRiver.query({}, function () {
                            $scope.model.rivers = curTerms
                            $scope.close_tab()
                        })
                    }, 500)
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_river.$update({}, function () {
                    $timeout(function () {
                        var curTerms = DataRiver.query({}, function () {
                            $scope.model.rivers = curTerms
                            $scope.close_tab()
                        })
                    }, 500)
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
            $scope.filter_options.data.push({'operator': 'is', 'type': 'regex', 'expression': ''})
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
            $scope.filter_data_parser_fields.data.push({'field_name': '', 'type': 'regex', 'expression': ''})
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
                'destination_type': 'elasticsearch',
                'reference': '',
                'operation': 'create',
                'clear_policy': '',
                'id': '',
                'host': '',
                'index': '',
                'type': '',
                'mapping': ''
            })
        }
    }]);

/***************************************************************************************
 *
 *                              ElastcSearch模板
 *
 **************************************************************************************/
manageControllers.controller('EsTmplCtrl', ['$scope', '$state', 'EsTmplTemplate', 'EsTmpl',
    function ($scope, $state, EsTmplTemplate, EsTmpl) {
        $scope.tabs = []
        $scope.template_data = EsTmplTemplate.query()
        $scope.model = {handlers: []}
        $scope.model.tmpls = EsTmpl.query();
    }]);

manageControllers.controller('EsTmplListCtrl', ['$scope', '$state', '$modal', 'EsTmpl', 'AlertService',
    function ($scope, $state, $modal, EsTmpl, AlertService) {
        $scope.tmplTableColumnDefinition = [
            {
                columnHeaderDisplayName: '搜索引擎数据模板名称',
                displayProperty: 'name',
                columnSearchProperty: 'name',
                visible: true
            },
            {
                columnHeaderTemplate: '主机地址',
                displayProperty: 'host',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'host'
            },
            {
                columnHeaderTemplate: '索引',
                displayProperty: 'index',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'index'
            },
            {
                columnHeaderTemplate: '文档类型',
                displayProperty: 'type',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'type'
            }, {
                columnHeaderTemplate: '文档主键',
                displayProperty: 'id',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'id'
            }, {
                columnHeaderTemplate: '文档映射',
                displayProperty: 'mapping',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'mapping'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)" tooltip="修改数据" tooltip-append-to-body=true><span class="glyphicon glyphicon-edit">' +
                '</span></button></div> <div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)" tooltip="删除数据" tooltip-append-to-body=true>' +
                '<span class="glyphicon glyphicon-remove"></span></button></div>',
                width: '5em'
            }
        ];

        $scope.create = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '创建搜索引擎模板') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了搜索引擎模板创建窗口')
                return
            }
            $scope.tabs.push({"title": '创建搜索引擎模板', "source_item": null});
            active_last($scope.tabs)
        };

        $scope.edit = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if (index >= 0 && $scope.tabs[index].title != '创建搜索引擎模板') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了搜索引擎模板编辑窗口')
                return
            }
            $scope.tabs.push({"title": item.name, "source_item": item});
            active_last($scope.tabs)
        };
        $scope.remove = function (item) {
            $scope.show_confirm('确定要删除数据吗？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }
                var cur_es_tmpl = new EsTmpl(item)
                cur_es_tmpl.$delete({}, function () {
                    for (var index = 0; index < $scope.model.tmpls.length; index++) {
                        var cur_river = $scope.model.tmpls[index];
                        if (cur_river == item) {
                            break;
                        }
                    }
                    $scope.model.tmpls.splice(index, 1)
                })
            })
        };
    }]);

manageControllers.controller('EsTmplEditCtrl', ['$scope', '$timeout', 'EsTmpl',
    function ($scope, $timeout, EsTmpl) {
        $scope.source_tmpl = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_tmpl = $scope.source_tmpl ? angular.copy($scope.source_tmpl) : {}
        $scope.mirror_tmpl.mapping_jsonstr = angular.toJson($scope.mirror_tmpl.mapping)
        $scope.op_type = $scope.source_tmpl ? 'edit' : 'create'
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
        $scope.submitted = false;
        $scope.save_es_tmpl = function (isValid) {
            $scope.submitted = true
            if (!isValid) {
                return
            }
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
            if ($scope.mirror_tmpl.mapping_jsonstr) {
                try {
                    copy_tmpl.mapping = angular.fromJson($scope.mirror_tmpl.mapping_jsonstr)
                }
                catch (e) {
                    $scope.show_error({statusText: 'The template mapping should be json string'})
                    return
                }
            }
            if ($scope.mirror_tmpl.name) {
                copy_tmpl.name = $scope.mirror_tmpl.name
            }
            if ($scope.op_type == 'create') {
                copy_tmpl.$save({}, function () {
                    $timeout(function () {
                        var cur_tmpls = EsTmpl.query({}, function () {
                            $scope.model.tmpls = cur_tmpls
                            $scope.close_tab()
                        })
                    }, 500)
                }, function (response) {
                    $scope.show_error(response)
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_tmpl.$update({}, function () {
                    $timeout(function () {
                        var cur_tmpls = EsTmpl.query({}, function () {
                            $scope.model.tmpls = cur_tmpls
                            $scope.close_tab()
                        })
                    }, 500)
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
        $scope.model = {handlers: []}
        $scope.update_list = function () {
            var cur_handlers = QueryHandler.query({}, function () {
                //clear_array($scope.handlers)
                //extend_array($scope.handlers, cur_handlers)
                $scope.model.handlers = cur_handlers
            });
        }
        var cur_handlers = QueryHandler.query({}, function () {
            //clear_array($scope.handlers)
            //extend_array($scope.handlers, cur_handlers)
            $scope.model.handlers = cur_handlers
        });
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
                columnSearchProperty: 'filter'
            },
            {
                columnHeaderTemplate: '请求解析器',
                displayProperty: 'data_parser',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'data_parser'
            },
            {
                columnHeaderTemplate: '数据仓库',
                displayProperty: 'destination',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'destination'
            },
            {
                columnHeaderTemplate: '响应数据处理',
                displayProperty: 'response',
                cellFilter: ['json', 'limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'response'
            },
            {
                columnHeaderDisplayName: '操作',
                template: '<div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="edit(item)" tooltip="修改数据" tooltip-append-to-body=true><span class="glyphicon glyphicon-edit">' +
                '</span></button></div> <div class="btn-group"><button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)" tooltip="删除数据" tooltip-append-to-body=true>' +
                '<span class="glyphicon glyphicon-remove"></span></button></div>',
                width: '5em'
            }
        ];

        $scope.create = function (item) {
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
            active_last($scope.tabs)
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
            for (var index in $scope.tabs) {
                if (index == $scope.tabs.length - 1) {
                    $scope.tabs[index].active = true
                }
                else {
                    $scope.tabs[index].active = false
                }
            }
            active_last($scope.tabs)
        };
        $scope.remove = function (item) {
            $scope.show_confirm('确定要删除数据吗？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }
                var cur_query_handler = new QueryHandler(item)
                cur_query_handler.$delete({}, function () {
                    for (var index = 0; index < $scope.model.handlers.length; index++) {
                        var cur_river = $scope.model.handlers[index];
                        if (cur_river == item) {
                            break;
                        }
                    }
                    $scope.model.handlers.splice(index, 1)
                })
            })
        };
    }]);

manageControllers.controller('QueryHandlerEditCtrl', ['$scope', '$state', '$modal', 'QueryHandlerFormat', 'QueryHandler', '$timeout',
    function ($scope, $state, $modal, QueryHandlerFormat, QueryHandler, $timeout) {
        $scope.source_handler = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_handler = QueryHandlerFormat.format_query_handler_to_display(angular.copy($scope.source_handler))
        $scope.op_type = $scope.source_handler ? 'edit' : 'create'
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
        $scope.submitted = false;
        $scope.save_query_handler = function (isValid) {
            $scope.submitted = true
            if (!isValid) {
                return
            }
            var copy_query_handler = QueryHandlerFormat.format_query_handler_to_model($scope.mirror_handler)

            if ($scope.op_type == 'create') {
                copy_query_handler.$save({}, function () {
                    $timeout(function () {
                        var curTerms = QueryHandler.query({}, function () {
                            //clear_array($scope.handlers)
                            //extend_array($scope.handlers, curTerms)
                            $scope.model.handlers = curTerms
                            $scope.close_tab()
                        })
                    }, 500)
                })
            }
            else if ($scope.op_type == 'edit') {
                copy_query_handler.$update({}, function () {
                    $timeout(function () {
                        var curTerms = QueryHandler.query({}, function () {
                            $scope.model.handlers = curTerms
                            //clear_array($scope.handlers)
                            //console.log('before: ', $scope.handlers)
                            //extend_array($scope.handlers, curTerms)
                            //console.log('after: ', $scope.handlers)
                            $scope.close_tab()
                        })
                    }, 500)
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
            $scope.filter_options.data.push({'operator': 'is', 'type': 'regex', 'expression': ''})
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
            $scope.data_parser_fields.data.push({'field_name': '', 'type': 'regex', 'expression': ''})
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

/***************************************************************************************
 *
 *                              系统参数配置
 *
 **************************************************************************************/
manageControllers.controller('SysParamCtrl', ['$scope', '$state', 'SysParamTemplate', 'SysParam', 'AlertService', '$timeout',
    function ($scope, $state, SysParamTemplate, SysParam, AlertService, $timeout) {
        $scope.template_data = SysParamTemplate.query()
        $scope.submitted = false
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

        $scope.save_sysparam = function (isValid) {
            $scope.submitted = true
            if (!isValid) {
                return
            }
            $scope.show_confirm('修改系统参数可能会导致搜索引擎系统异常，是否确认？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }

                $scope.sysparam.$update(function () {
                    $scope.submitted = false
                    $timeout(function () {
                        $scope.sysparam = SysParam.query({}, function () {
                            $scope.host_grid.data = $scope.sysparam.manager.hosts
                            $scope.system_edit_form.$setPristine()
                        });
                    }, 1000)
                }, function (response) {
                    $scope.show_error(response)
                })
            })
        }
    }]);


/***************************************************************************************
 *
 *                              集群管理
 *
 **************************************************************************************/
manageControllers.controller('ProcessCtrl', ['$scope', '$state', 'Process', 'ProcessAction', '$timeout',
    function ($scope, $state, Process, ProcessAction, $timeout) {
        $scope.hosts = Process.query();
        $scope.doAction = function (host, action_name, process_name) {
            $scope.show_confirm('此操作可能会导致搜索引擎系统异常，是否确认？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }

                var action = new ProcessAction({host: host, action: action_name, name: process_name})
                action.$do({}, function () {
                    $scope.hosts = Process.query()
                }, function (response) {
                    $scope.show_error(response)
                })
            })
        }
        $scope.viewLog = function (host, process_name) {
            var action = new ProcessAction({host: host, action: 'get_log', name: process_name})
            var log_info = action.$get({}, function () {
                console.log(log_info)
                var url = $state.href('processlog', {logs: log_info});
                window.open(url, '_blank');
                //$state.go('processlog', {logs: log_info})
            })
        }
    }]);


/***************************************************************************************
 *
 *                              消息管理
 *
 **************************************************************************************/

manageControllers.controller('MessageCtrl', ['$scope', '$state', 'MessageTemplate', 'Message',
    function ($scope, $state, MessageTemplate, Message) {
        $scope.tabs = []
        $scope.template_data = MessageTemplate.query()
        $scope.messages = Message.query();
    }]);

manageControllers.controller('MessageListCtrl', ['$scope', '$state', '$modal', 'Message', 'AlertService',
    function ($scope, $state, $modal, Message, AlertService) {
        $scope.messageTableColumnDefinition = [
            {
                columnHeaderDisplayName: '消息类型',
                displayProperty: 'type',
                sortKey: 'type',
                columnSearchProperty: 'type',
                visible: true
            },
            {
                columnHeaderTemplate: '消息源',
                sortKey: 'source',
                displayProperty: 'source',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'source'
            },
            {
                columnHeaderTemplate: '消息目的地',
                displayProperty: 'destination',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                sortKey: 'destination',
                columnSearchProperty: 'destination'
            },
            {
                columnHeaderTemplate: '消息体',
                displayProperty: 'body',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                columnSearchProperty: 'body'
            }, {
                columnHeaderTemplate: '消息发送时间',
                displayProperty: 'send_time',
                cellFilter: ['limitTo:100', 'add_dots:4'],
                sortKey: 'send_time',
                columnSearchProperty: 'send_time'
            }
        ];

        $scope.create = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '消息发送') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了消息发送窗口')
                return
            }
            $scope.tabs.push({"title": '消息发送', "source_item": null});
            active_last($scope.tabs)
        };
    }]);

manageControllers.controller('MessageEditCtrl', ['$scope', '$timeout', 'Message',
    function ($scope, $timeout, Message) {
        $scope.source_message = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_message = $scope.source_message ? angular.copy($scope.source_message) :
        {'type': 'update_log_level'}
        $scope.op_type = 'create'
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
        $scope.submitted = false
        $scope.save_message = function (isValid) {
            $scope.submitted = true
            if (!isValid) {
                return
            }
            $scope.show_confirm('你确定要发送消息吗？', function (is_confirm) {
                    if (!is_confirm) {
                        return
                    }

                    var copy_message = new Message($scope.mirror_message)
                    if ($scope.op_type == 'create') {
                        copy_message.$save({}, function () {
                            $scope.close_tab()
                            $timeout(function () {
                                var cur_messages = Message.query({}, function () {
                                    clear_array($scope.messages)
                                    extend_array($scope.messages, cur_messages)
                                })
                            }, 500)
                        }, function (response) {
                            $scope.show_error(response)
                        })
                    }
                }
            )
        }
    }]);

/***************************************************************************************
 *
 *                              ANSJ配置
 *
 **************************************************************************************/
manageControllers.controller('AnsjCtrl', ['$scope', '$state', 'AnsjTemplate', 'Ansj', 'AlertService',
    function ($scope, $state, AnsjTemplate, Ansj, AlertService) {
        $scope.template_data = AnsjTemplate.query()

        $scope.ambiguity_term_grid = {
            enableRowSelection: true,
            enableRowHeaderSelection: false,
            multiSelect: false,
            modifierKeysToMultiSelect: false,
            noUnselect: true
        }

        $scope.ambiguity_term_grid.columnDefs = [
            {
                name: 'term',
                displayName: '分词'
            },
            {
                name: 'speech',
                displayName: '词性',
                editableCellTemplate: 'ui-grid/dropdownEditor',
                width: '50%',
                editDropdownValueLabel: 'display_value',
                editDropdownRowEntityOptionsArrayPath: 'speech_options',
                //editDropdownOptionsArray:$scope.template_data.ansj_speechs,
                editDropdownIdLabel: 'value'
            }
        ];
        $scope.seg = {'type': 'user_define', 'operator': 'add'}
        $scope.ambiguity_term_grid.data = []

        $scope.ambiguity_term_grid.onRegisterApi = function (gridApi) {
            $scope.ambiguity_term_grid.gridApi = gridApi;
            gridApi.edit.on.beginCellEdit($scope, function (rowEntity, colDef, newValue, oldValue) {
            });
        };
        $scope.delete_ambiguity_term = function () {
            var select_rows = $scope.ambiguity_term_grid.gridApi.selection.getSelectedRows()
            if (!select_rows || select_rows.length == 0) {
                AlertService.add('danger', '请先选择一行记录')
            }
            else {
                delete_array_element(select_rows[0], $scope.ambiguity_term_grid.data)
            }
        }

        $scope.add_ambiguity_term = function () {
            $scope.ambiguity_term_grid.data.push({
                'term': '',
                'speech': 'n',
                'speech_options': $scope.template_data.ansj_speechs
            })
        }

        $scope.save_ansj = function () {
            if ($scope.seg.type == 'user_define') {
                $scope.seg.text = $scope.seg.user_define_text
            }
            else if ($scope.seg.type == 'ambiguity') {
                $scope.seg.text = $scope.seg.ambiguity_text + '-'
                for (var index in $scope.ambiguity_term_grid.data) {
                    var term = $scope.ambiguity_term_grid.data[index]
                    $scope.seg.text += term.term + ',' + term.speech + ','
                }
                $scope.seg.text = $scope.seg.text.substr(0, $scope.seg.text.length - 1)
            }
            var asnj = new Ansj($scope.seg)
            if ($scope.seg.operator == 'add') {
                asnj.$add({}, function () {
                    $scope.seg = {'type': 'user_define'}
                    $scope.ambiguity_term_grid.data = []
                }, function (response) {
                    $scope.show_error(response)
                });

            }
            else if ($scope.seg.operator == 'delete') {
                asnj.$delete({}, function () {
                    $scope.seg = {'type': 'user_define'}
                    $scope.ambiguity_term_grid.data = []
                }, function (response) {
                    $scope.show_error(response)
                });
            }
        }
    }
])

/***************************************************************************************
 *
 *                              拼写建议
 *
 **************************************************************************************/

manageControllers.controller('SuggestCtrl', ['$scope', 'SuggestTemplate', '$resource',
    function ($scope, SuggestTemplate, $resource) {
        $scope.tabs = []
        $scope.template_data = SuggestTemplate.query()
        $scope.adminID = ''
        $scope.adminIDs = []
        $scope.terms = []
        $scope.Suggest = $resource('suggestterms/:adminID/:word', {}, {
            'query': {method: 'GET', isArray: true},
            'save': {method: 'POST'},
            'delete': {method: 'DELETE'}
        });
        $scope.SuggestOperation = $resource('suggestterms/:adminID/operations/:operation', {}, {
            'init': {method: 'POST'}
        });
    }]);

manageControllers.controller('SuggestListCtrl', ['$scope', '$state', '$modal', '$resource', 'AlertService',
    function ($scope, $state, $modal, $resource, AlertService) {
        $scope.termTableColumnDefinition = [
            {
                columnHeaderTemplate: '建议词来源',
                sortKey: 'source_type',
                displayProperty: 'source_type',
                columnSearchProperty: 'source_type'
            },
            {
                columnHeaderTemplate: '建议词',
                displayProperty: 'word',
                columnSearchProperty: 'word'
            }, {
                columnHeaderDisplayName: '操作',
                template: '</button> <button type="button" class="btn btn-primary btn-xs" ng-click="remove(item)" tooltip="删除行数据" tooltip-append-to-body=true>' +
                '<span class="glyphicon glyphicon-remove"></span></button>',
                width: '20em'
            }
        ];

        $scope.query = function () {
            $scope.adminIDs[0] = $scope.adminID
            if (!$scope.adminID) {
                clear_array($scope.terms);
                return
            }
            $scope.Suggest = $resource('suggestterms/:adminID/:word', {
                adminID: $scope.adminID
            }, {
                'query': {method: 'GET', isArray: false},
                'save': {method: 'POST'},
                'delete': {method: 'DELETE'}
            });

            var curTerms = $scope.Suggest.query({
                adminID: $scope.adminID, whoami: 'god',
                size: 5000
            }, function () {
                clear_array($scope.terms)
                extend_array($scope.terms, curTerms['root'])
            }, function (response) {
                $scope.show_error(response)
                return
            })
        };

        $scope.create = function (item) {
            var is_open = false
            for (var index in $scope.tabs) {
                if ($scope.tabs[index].title == '添加建议词') {
                    is_open = true
                    break
                }
            }
            if (is_open) {
                AlertService.add('danger', '已经打开了添加建议词窗口')
                return
            }
            $scope.tabs.push({"title": '添加建议词', "source_item": null});
            active_last($scope.tabs)
        };
        $scope.remove = function (item) {
            $scope.show_confirm('确定要删除数据吗？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }
                var term = new $scope.Suggest(item)
                var copy_item = angular.copy(item)
                copy_item.adminID = $scope.adminID
                term.$delete({word: term.word}, function () {
                    for (var index = 0; index < $scope.terms.length; index++) {
                        var cur_river = $scope.terms[index];
                        if (cur_river == item) {
                            break;
                        }
                    }
                    $scope.terms.splice(index, 1)
                })
            })
        };
        $scope.init = function () {
            $scope.show_confirm('初始化操作会导致自动分词扫描形成的建议词被初始化，是否确定？', function (is_confirm) {
                if (!is_confirm) {
                    return
                }
                var operation = new $scope.SuggestOperation()
                operation.$init({adminID: $scope.adminID, operation: "init"}, function () {
                })
            })
        }
    }
]);

manageControllers.controller('SuggestEditCtrl', ['$scope', '$timeout',
    function ($scope, $timeout) {
        $scope.source_term = $scope.tabs[$scope.tabs.length - 1].source_item
        $scope.mirror_term = $scope.source_term ? angular.copy($scope.source_term) : {
            'product_type': 'product',
            'source_type': '1'
        }
        $scope.op_type = 'create'
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

        $scope.submitted = false;
        $scope.save_term = function (isValid) {
            $scope.submitted = true;
            if (!isValid) {
                return
            }
            var copy_message = new $scope.Suggest($scope.mirror_term)
            if ($scope.op_type == 'create') {
                copy_message.$save({adminID: $scope.adminIDs[0]}, function () {
                    $scope.close_tab()
                    $timeout(function () {
                        var curTerms = $scope.Suggest.query({adminID: $scope.adminIDs[0]}, function () {
                            clear_array($scope.terms)
                            extend_array($scope.terms, curTerms)
                        })
                    }, 500)
                })
            }
        }
    }]);


/**
 * 确认对话框Controller
 */
manageControllers.controller('ConfirmModalInstanceCtrl', function ($scope, $modalInstance, information) {
    $scope.infomation = information
    $scope.ok = function () {
        $modalInstance.close(true);
    };

    $scope.cancel = function () {
        $modalInstance.close(false);
    };
});

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

/**
 * 清空数组
 * @param arrray
 */
function clear_array(array) {
    array.splice(0)
}

/**
 * 将原数组中得数据添加到目的数组中
 * @param dstArray
 * @param srcArray
 */
function extend_array(dstArray, srcArray) {
    Array.prototype.push.apply(dstArray, srcArray);
}

/**
 * 将最后一个tab设为active
 * @param tab_array
 */
function active_last(tab_array) {
    for (var index in tab_array) {
        if (index == tab_array.length - 1) {
            tab_array[index].active = true
        }
        else {
            tab_array[index].active = false
        }
    }
}
