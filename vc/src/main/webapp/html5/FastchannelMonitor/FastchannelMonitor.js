/**
 * Controller SankhyaJS para Monitor de Integracao Fastchannel
 * Padrao correto para HTML5 screens no Sankhya OM
 */

// Definir controller no escopo global (padrao SankhyaJS)
function FastchannelMonitorController($scope, $timeout) {

    // === State ===
    $scope.apiHealth = { status: 'Verificando...', online: null };
    $scope.stats = { success24h: 0, error24h: 0, successRate: 100 };
    $scope.logs = [];
    $scope.syncing = false;
    $scope.message = null;
    $scope.messageType = 'success';

    // === Initialization ===
    $scope.init = function () {
        console.log('FastchannelMonitorController initialized');
        $scope.checkHealth();
        $scope.loadStats();
        $scope.loadLogs();
    };

    // === API Health Check ===
    $scope.checkHealth = function () {
        console.log('Checking API health...');

        callSankhyaService('FastchannelMonitorSP', 'getApiHealth', {}, function (response) {
            if (response && response.responseBody) {
                $scope.apiHealth = response.responseBody;
                $scope.$apply();
            }
        }, function (error) {
            console.error('Health check failed:', error);
            $scope.apiHealth = { status: 'OFFLINE', online: false };
            $scope.$apply();
        });
    };

    // === Load Statistics ===
    $scope.loadStats = function () {
        callSankhyaService('FastchannelMonitorSP', 'getStatistics', {}, function (response) {
            if (response && response.responseBody) {
                $scope.stats = response.responseBody;
                $scope.$apply();
            }
        }, function (error) {
            console.error('Failed to load stats:', error);
        });
    };

    // === Load Logs ===
    $scope.loadLogs = function () {
        callSankhyaService('FastchannelMonitorSP', 'getIntegrationLogs', { limit: 50 }, function (response) {
            if (response && response.responseBody && response.responseBody.logs) {
                $scope.logs = response.responseBody.logs;
                $scope.$apply();
            }
        }, function (error) {
            console.error('Failed to load logs:', error);
            showMessage('Erro ao carregar logs', 'error');
        });
    };

    // === Force Sync ===
    $scope.forceSync = function () {
        if ($scope.syncing) return;

        $scope.syncing = true;
        showMessage('Iniciando sincronizacao...', 'success');

        callSankhyaService('FastchannelIntegrationSP', 'syncAll', {}, function (response) {
            $scope.syncing = false;

            if (response && response.responseBody) {
                var result = response.responseBody;
                if (result.success) {
                    showMessage(result.message || 'Sincronizacao concluida!', 'success');
                } else {
                    showMessage(result.message || 'Erro na sincronizacao', 'error');
                }
            }

            // Reload data
            $scope.loadStats();
            $scope.loadLogs();
            $scope.$apply();

        }, function (error) {
            $scope.syncing = false;
            console.error('Sync failed:', error);
            showMessage('Erro na sincronizacao: ' + (error.message || 'Erro desconhecido'), 'error');
            $scope.$apply();
        });
    };

    // === Helper: Show Toast Message ===
    function showMessage(msg, type) {
        $scope.message = msg;
        $scope.messageType = type || 'success';
        $scope.$apply();

        $timeout(function () {
            $scope.message = null;
        }, 5000);
    }

    // === Helper: Call Sankhya Service ===
    function callSankhyaService(serviceName, methodName, params, successCallback, errorCallback) {
        var fullServiceName = serviceName + '.' + methodName;

        try {
            // Tentar usar ServiceProxy (SankhyaJS padrao)
            if (typeof ServiceProxy !== 'undefined' && ServiceProxy.createCall) {
                var call = ServiceProxy.createCall(fullServiceName, params);
                call.execute(function (response) {
                    if (successCallback) successCallback(response);
                }, function (error) {
                    if (errorCallback) errorCallback(error);
                });
            }
            // Fallback: Usar SWServiceInvoker
            else if (typeof SWServiceInvoker !== 'undefined') {
                SWServiceInvoker.invoke(fullServiceName, params, function (response) {
                    if (successCallback) successCallback({ responseBody: response });
                }, function (error) {
                    if (errorCallback) errorCallback(error);
                });
            }
            // Fallback: AJAX direto
            else {
                var xhr = new XMLHttpRequest();
                var url = '/mge/service.sbr?serviceName=' + fullServiceName + '&outputType=json';

                xhr.open('POST', url, true);
                xhr.setRequestHeader('Content-Type', 'application/json');

                xhr.onload = function () {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            var response = JSON.parse(xhr.responseText);
                            if (successCallback) successCallback(response);
                        } catch (e) {
                            if (errorCallback) errorCallback({ message: 'Parse error' });
                        }
                    } else {
                        if (errorCallback) errorCallback({ message: 'HTTP ' + xhr.status });
                    }
                };

                xhr.onerror = function () {
                    if (errorCallback) errorCallback({ message: 'Network error' });
                };

                xhr.send(JSON.stringify({ requestBody: params }));
            }
        } catch (e) {
            console.error('Service call error:', e);
            if (errorCallback) errorCallback(e);
        }
    }
}

// Registrar controller no AngularJS (compatibilidade SankhyaJS)
FastchannelMonitorController.$inject = ['$scope', '$timeout'];

