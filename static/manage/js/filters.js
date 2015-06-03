'use strict';
/* Filters */
var manageFilters = angular.module('manageFilters', [])
manageFilters.filter('add_dots', function () {
    return function (input, dot_num) {
        var dot_str = ''
        if (!input) {
            return ''
        }
        if (input.length < 100) {
            return input
        }
        for (var i = 0; i < dot_num; i++) {
            dot_str += '.'
        }
        return input + ' ' + dot_str;
    };
});