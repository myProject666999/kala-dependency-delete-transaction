(function(window) {

  var KalaHost = window.location.protocol + "//" + window.location.hostname + (window.location.port ? ':' + window.location.port : '');
  var Version = 'v1';
  var ApiBase = KalaHost + "/api/" + Version + "/";
  var ApiJobEndpoint = ApiBase + "job/";

  var service = {
    getJobs: function() {
      return fetch(ApiJobEndpoint)
        .then(function(response) {
          return response.json()
        })
        .catch(function(ex) {
          console.error('parsing jobs failed: ', ex)
        });
    },
    getJob: function(id) {
      return fetch(ApiJobEndpoint + id + '/')
        .then(function(response) {
          return response.json()
        })
        .then(function(json) {
          if (json.error) {
            throw new Error(json.error);
          }
          return json.job;
        })
        .catch(function(ex) {
          console.error('parsing job failed: ', ex)
          throw new Error(ex);
        });
    },
    disableJob: function(id) {
      return fetch(ApiJobEndpoint + 'disable/' + id + '/', {
        method: 'post'
      })
        .catch(function(ex) {
          console.error('disabling job failed: ', ex)
        })
    },
    enableJob: function(id) {
      return fetch(ApiJobEndpoint + 'enable/' + id + '/', {
        method: 'post'
      })
        .catch(function(ex) {
          console.error('enabling job failed: ', ex)
        })
    },
    runJob: function(id) {
      return fetch(ApiJobEndpoint + 'start/' + id + '/', {
        method: 'post'
      })
        .catch(function(ex) {
          console.error('starting job failed: ', ex)
        })
    },
    deleteJob: function(id) {
      return fetch(ApiJobEndpoint + id + '/', {
        method: 'delete'
      })
        .then(function(response) {
          // 204 means Kala confirmed the delete and its dependency repairs.
          // 404 means it was already gone, which is fine. Anything else means
          // it did not happen; surface the reason instead of pretending.
          if (response.status === 204 || response.status === 404) {
            return { ok: true };
          }
          return response.json()
            .catch(function() { return {}; })
            .then(function(json) {
              throw new Error(json.error || ('Delete failed with status ' + response.status));
            });
        });
    },
    createJob: function(job) {
      return fetch(ApiJobEndpoint, {
        method: 'post',
        body: JSON.stringify(job)
      })
        .then(function(response) {
          return response.json()
        })
        .then(function(json) {
          if (json.error) {
            throw new Error(json.error);
          }
          return json.id;
        })
        .catch(function(ex) {
          console.error('creating job failed: ', ex)
        })
    },
    jobStats: function(id) {
      return fetch(ApiJobEndpoint + 'stats/' + id + '/')
        .catch(function(ex) {
          console.error('getting job stats failed: ', ex)
        })
    },
    metrics: function() {
      return fetch(ApiBase + 'stats/')
        .then(function(resp) {
          return resp.json()
        })
        .then(function(json) {
          return json.Stats
        })
        .catch(function(ex) {
          console.error('getting metrics failed: ', ex)
        })
    },
  }

  this.kala = service;

})(this);
