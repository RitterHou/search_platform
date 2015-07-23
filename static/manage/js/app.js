'use strict';

/* App Module */

var manageApp = angular.module('manageApp', ['ui.tree', 'ui.router', 'ui.bootstrap', 'ngSanitize', 'adaptv.adaptStrap',
    'manageControllers', 'manageServices', 'manageFilters', 'ui.grid', 'ui.grid.edit',
    'ui.grid.selection', 'ui.grid.autoResize', 'ngMessages']);

//manageApp.config(['$routeProvider',
//    function ($routeProvider) {
//        $routeProvider.
//            when('/manage', {
//                templateUrl: '../static/manage/partials/main.html',
//                controller: 'ManageTreeCtrl'
//            }).otherwise({
//                redirectTo: '/manage'
//            });
//        ;
//    }]);
manageApp.run(
    ['$rootScope', '$state', '$stateParams',
        function ($rootScope, $state, $stateParams) {

            // It's very handy to add references to $state and $stateParams to the $rootScope
            // so that you can access them from any scope within your applications.For example,
            // <li ng-class="{ active: $state.includes('contacts.list') }"> will set the <li>
            // to active whenever 'contacts.list' or one of its decendents is active.
            $rootScope.$state = $state;
            $rootScope.$stateParams = $stateParams;
            $rootScope.formatUtils = {
                /**
                 * 将实际值转化为view显示值
                 * @param format_templs
                 * @param value
                 */
                toDisplayValue: function (format_templs, value) {
                    if (angular.isArray(format_templs)) {
                        for (var index in format_templs) {
                            if (format_templs[index].value == value) {
                                return format_templs[index].display_value
                            }
                        }
                    }
                    else if (angular.isObject(format_templs) && 'value' in format_templs) {
                        return format_templs.display_value
                    }
                    return value
                }
            }
            $rootScope.interacted = function (submitted, field) {
                return submitted;
            };
        }
    ]
)
manageApp.config(function ($stateProvider, $urlRouterProvider) {
    //
    // For any unmatched url, redirect to /state1
    $urlRouterProvider.otherwise("/node/103/info");
    //
    // Now set up the states
    $stateProvider
        .state('node', {
            url: "/node",
            templateUrl: "../static/manage/partials/main.html",
            controller: 'ManageTreeCtrl'
        }).state('node.101', {
            url: "/101",
            templateUrl: "../static/manage/partials/datariver/datariver.html",
            controller: 'DataRiverCtrl'
        }).state('node.101.info', {
            url: "/info",
            views: {
                "datariver_list": {
                    templateUrl: "../static/manage/partials/datariver/datariver.list.html",
                    controller: 'DataRiverListCtrl'
                },
                "datariver_op": {
                    templateUrl: "../static/manage/partials/datariver/datariver.river.edit.html",
                    controller: 'DataRiverEditCtrl'
                }
            }
        }).state('node.101.info.edit', {
            views: {
                "notification": {
                    templateUrl: "../static/manage/partials/datariver/datariver.notification.html",
                    controller: 'DataRiverNotificationCtrl'
                },
                "source": {
                    templateUrl: "../static/manage/partials/datariver/datariver.source.html",
                    controller: 'DataRiverSourceCtrl'
                },
                "destination": {
                    templateUrl: "../static/manage/partials/datariver/datariver.destination.html",
                    controller: 'DataRiverDestinationCtrl'
                }
            }
        }).state('node.103', {
            url: "/103",
            templateUrl: "../static/manage/partials/estmpl/estmpl.html",
            controller: 'EsTmplCtrl'
        }).state('node.103.info', {
            url: "/info",
            views: {
                "estmpl_list": {
                    templateUrl: "../static/manage/partials/estmpl/estmpl.list.html",
                    controller: 'EsTmplListCtrl'
                },
                "estmpl_op": {
                    templateUrl: "../static/manage/partials/estmpl/estmpl.edit.html",
                    controller: 'EsTmplEditCtrl'
                }
            }
        })
        .state('node.102', {
            url: "/102",
            templateUrl: "../static/manage/partials/queryhandler/queryhandler.html",
            controller: 'QueryHandlerCtrl'
        }).state('node.102.info', {
            url: "/info",
            views: {
                "queryhandler_list": {
                    templateUrl: "../static/manage/partials/queryhandler/queryhandler.list.html",
                    controller: 'QueryHandlerListCtrl'
                },
                "queryhandler_op": {
                    templateUrl: "../static/manage/partials/queryhandler/queryhandler.edit.html",
                    controller: 'QueryHandlerEditCtrl'
                }
            }
        })
        .state('node.202', {
            url: "/202",
            templateUrl: "../static/manage/partials/sysparam/sysparam.html",
            controller: 'SysParamCtrl'
        }).state('node.202.info', {
            url: "/info",
            views: {
                "sysparam_op": {
                    templateUrl: "../static/manage/partials/sysparam/sysparam.edit.html",
                    controller: 'SysParamCtrl'
                }
            }
        })
        .state('node.201', {
            url: "/201",
            templateUrl: "../static/manage/partials/process/process.html",
            controller: 'ProcessCtrl'
        })
        .state('node.201.info', {
            url: "/info",
            templateUrl: "../static/manage/partials/process/process.html",
            controller: 'ProcessCtrl'
        })
        .state('processlog', {
            url: "/process/log",
            templateUrl: "../static/manage/partials/process/process.log.html",
            controller: 'ProcessCtrl'
        })
        .state('node.203', {
            url: "/203",
            templateUrl: "../static/manage/partials/message/message.html",
            controller: 'MessageCtrl'
        })
        .state('node.203.info', {
            url: "/info",
            views: {
                "message_list": {
                    templateUrl: "../static/manage/partials/message/message.list.html",
                    controller: 'MessageListCtrl'
                },
                "message_op": {
                    templateUrl: "../static/manage/partials/message/message.edit.html",
                    controller: 'MessageEditCtrl'
                }
            }
        })
        .state('node.204', {
            url: "/204",
            templateUrl: "../static/manage/partials/ansj/ansj.html",
            controller: 'AnsjCtrl'
        }).state('node.204.info', {
            url: "/info",
            views: {
                "ansj_op": {
                    templateUrl: "../static/manage/partials/ansj/ansj.edit.html",
                    controller: 'AnsjCtrl'
                }
            }
        })
        .state('node.104', {
            url: "/104",
            templateUrl: "../static/manage/partials/suggest/suggest.html",
            controller: 'SuggestCtrl'
        })
        .state('node.104.info', {
            url: "/info",
            views: {
                "suggest_term_list": {
                    templateUrl: "../static/manage/partials/suggest/suggest.list.html",
                    controller: 'SuggestListCtrl'
                },
                "suggest_term_op": {
                    templateUrl: "../static/manage/partials/suggest/suggest.edit.html",
                    controller: 'SuggestEditCtrl'
                }
            }
        })
});