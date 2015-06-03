'use strict';

/* App Module */

var manageApp = angular.module('manageApp', ['ui.tree', 'ui.router', 'ui.bootstrap', 'ngSanitize', 'adaptv.adaptStrap',
    'manageControllers', 'manageServices', 'manageFilters', 'ui.grid', 'ui.grid.edit',
    'ui.grid.selection', 'ui.grid.autoResize']);

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
        }
    ]
)
manageApp.config(function ($stateProvider, $urlRouterProvider) {
    //
    // For any unmatched url, redirect to /state1
    $urlRouterProvider.otherwise("/node");
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
});