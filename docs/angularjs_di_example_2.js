var app = angular.module("DemoApp", []);

var myFunc = function(s, h) {
    h.get('https://api.github.com/users/angular/repos')
        .success(function (repos) {
        s.repos = repos;
    });
};

// Controller is injected with $scope and $http as dependencies
app.controller('DemoController', ['$scope', '$http', myFunc]);
